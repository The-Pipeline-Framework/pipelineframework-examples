package org.pipelineframework.connector.http;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorOperationTypeContract;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderManifest;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Wires TPF's JVM ecosystem {@link HttpConnector} ({@code http.client} v1) in {@code DIRECT}
 * canonical JSON mode for Tier-3 mixed-topology polyglot proofs (ADR-0092 §4).
 */
public final class ProofHttpConnectorFactory {

    private static final Pattern PATH_PARAM = Pattern.compile("\\{([^}]+)}");
    private static final HttpWireSchema DEFAULT_PARAM_SCHEMA = new HttpWireSchema("{\"type\":\"string\"}");

    private ProofHttpConnectorFactory() {
    }

    public record OperationSpec(
        String operationId,
        ConnectorOperationKind kind,
        int majorVersion,
        String inputType,
        String outputType,
        String method,
        String pathTemplate,
        List<String> queryParams,
        Optional<String> idempotencyHeader,
        String successStatus,
        Optional<String> authScheme
    ) {
        public OperationSpec {
            Objects.requireNonNull(operationId, "operationId must not be null");
            Objects.requireNonNull(kind, "kind must not be null");
            Objects.requireNonNull(inputType, "inputType must not be null");
            Objects.requireNonNull(outputType, "outputType must not be null");
            Objects.requireNonNull(method, "method must not be null");
            Objects.requireNonNull(pathTemplate, "pathTemplate must not be null");
            queryParams = queryParams == null ? List.of() : List.copyOf(queryParams);
            idempotencyHeader = idempotencyHeader == null ? Optional.empty() : idempotencyHeader;
            successStatus = (successStatus == null || successStatus.isBlank()) ? "200" : successStatus.trim();
            authScheme = authScheme == null ? Optional.empty() : authScheme;
        }
    }

    public static HttpConnector create(CanonicalTypeCatalogue catalogue, List<OperationSpec> specs) {
        Objects.requireNonNull(catalogue, "catalogue must not be null");
        Objects.requireNonNull(specs, "specs must not be null");

        List<HttpOperationPin> pins = new ArrayList<>();
        List<HttpOperationRepresentationBinding> bindings = new ArrayList<>();
        List<ConnectorOperationDescriptor> descriptors = new ArrayList<>();

        for (OperationSpec spec : specs) {
            String method = spec.method().trim().toUpperCase(Locale.ROOT);
            HttpWireSchema requestSchema = new HttpWireSchema(catalogue.schema(spec.inputType()));
            HttpWireSchema responseSchema = new HttpWireSchema(catalogue.schema(spec.outputType()));
            String sourceFp = sha256Hex(spec.operationId() + ":" + method + ":" + spec.pathTemplate());
            String directFp = sha256Hex("direct:" + spec.operationId());

            List<HttpParameterPin> params = new ArrayList<>();
            Set<String> pathNames = new LinkedHashSet<>();
            Matcher matcher = PATH_PARAM.matcher(spec.pathTemplate());
            while (matcher.find()) {
                String name = matcher.group(1).trim();
                if (pathNames.add(name)) {
                    params.add(new HttpParameterPin(
                        name, HttpParameterLocation.PATH, name,
                        HttpParameterStyle.SIMPLE, false, true, false,
                        fieldSchema(requestSchema, name)));
                }
            }
            for (String q : spec.queryParams()) {
                if (q != null && !q.isBlank()) {
                    String name = q.trim();
                    params.add(new HttpParameterPin(
                        name, HttpParameterLocation.QUERY, name,
                        HttpParameterStyle.FORM, false, isRequired(requestSchema, name), false,
                        fieldSchema(requestSchema, name)));
                }
            }

            boolean sendsBody = !"GET".equals(method) && !"DELETE".equals(method) && !"HEAD".equals(method);
            Optional<HttpRequestBodyPin> requestBody = sendsBody
                ? Optional.of(new HttpRequestBodyPin("application/json", Optional.empty(), true, requestSchema))
                : Optional.empty();

            String requestMappingKey = "http." + spec.operationId() + ".request";
            String responseMappingKey = "http." + spec.operationId() + ".response";

            HttpSecurityConstraint security = spec.authScheme()
                .map(scheme -> new HttpSecurityConstraint(List.of(
                    new HttpSecurityRequirement(scheme, List.of(),
                        List.of(new HttpAuthorizationTarget(HttpParameterLocation.HEADER, "Authorization"))))))
                .orElseGet(HttpSecurityConstraint::none);

            List<HttpResponsePin> responses;
            if (spec.kind() == ConnectorOperationKind.QUERY) {
                responses = List.of(
                    new HttpResponsePin(spec.successStatus(), Optional.of("application/json"), HttpResponseOutcome.RESULT,
                        Optional.of(responseMappingKey), Optional.empty(), Optional.empty(),
                        Optional.of(responseSchema)),
                    new HttpResponsePin("404", Optional.empty(), HttpResponseOutcome.EMPTY,
                        Optional.empty(), Optional.of("http-not-found"), Optional.empty(), Optional.empty()),
                    new HttpResponsePin("401", Optional.empty(), HttpResponseOutcome.AUTHENTICATION_REQUIRED,
                        Optional.empty(), Optional.of("http-unauthorized"), Optional.empty(), Optional.empty()),
                    new HttpResponsePin("503", Optional.empty(), HttpResponseOutcome.RETRYABLE_FAILURE,
                        Optional.empty(), Optional.of("http-service-unavailable"), Optional.empty(), Optional.empty()));
            } else {
                responses = List.of(
                    new HttpResponsePin(spec.successStatus(), Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                        Optional.of(responseMappingKey), Optional.empty(),
                        Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED),
                        Optional.of(responseSchema)),
                    new HttpResponsePin("200", Optional.of("application/json"), HttpResponseOutcome.SUCCEEDED,
                        Optional.of(responseMappingKey), Optional.empty(),
                        Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED),
                        Optional.of(responseSchema)),
                    new HttpResponsePin("401", Optional.empty(), HttpResponseOutcome.AUTHENTICATION_REQUIRED,
                        Optional.empty(), Optional.of("http-unauthorized"), Optional.empty(), Optional.empty()),
                    new HttpResponsePin("422", Optional.empty(), HttpResponseOutcome.TERMINAL_FAILURE,
                        Optional.empty(), Optional.of("http-unprocessable"), Optional.empty(), Optional.empty()),
                    new HttpResponsePin("503", Optional.empty(), HttpResponseOutcome.RETRYABLE_FAILURE,
                        Optional.empty(), Optional.of("http-service-unavailable"), Optional.empty(), Optional.empty()));
                if ("200".equals(spec.successStatus())) {
                    responses = responses.stream()
                        .collect(Collectors.toMap(
                            HttpResponsePin::status, r -> r, (first, second) -> first, LinkedHashMap::new))
                        .values().stream().toList();
                }
            }

            Optional<HttpProviderIdempotencyKeyTarget> idempotencyTarget = spec.idempotencyHeader()
                .filter(h -> !h.isBlank())
                .map(h -> new HttpProviderIdempotencyKeyTarget(HttpParameterLocation.HEADER, h.trim()));

            pins.add(new HttpOperationPin(
                spec.operationId(), spec.kind(), spec.majorVersion(),
                spec.inputType(), spec.outputType(), method, spec.pathTemplate(),
                params, requestBody, responses, security,
                requestSchema, requestMappingKey, idempotencyTarget, List.of(), sourceFp));

            bindings.add(new HttpOperationRepresentationBinding(
                requestMappingKey, HttpRepresentationMode.DIRECT, Optional.empty(), Optional.empty(), directFp));
            bindings.add(new HttpOperationRepresentationBinding(
                responseMappingKey, HttpRepresentationMode.DIRECT, Optional.empty(), Optional.empty(), directFp));

            if (spec.kind() == ConnectorOperationKind.QUERY) {
                descriptors.add(new ConnectorOperationDescriptor(
                    spec.operationId(), ConnectorOperationKind.QUERY, spec.majorVersion(),
                    Optional.empty(), Optional.empty(),
                    Optional.of(QueryCapabilities.conservative()),
                    Optional.of(QueryOperationCardinality.ONE_TO_ONE),
                    Optional.of(new ConnectorOperationTypeContract(spec.inputType(), Optional.of(spec.outputType()))),
                    List.of()));
            } else {
                CommandCapabilities capabilities = new CommandCapabilities(
                    idempotencyTarget.isPresent(), idempotencyTarget.isPresent(), false,
                    CommandExecutionPosture.UNSPECIFIED, CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
                    false, Set.of());
                descriptors.add(new ConnectorOperationDescriptor(
                    spec.operationId(), ConnectorOperationKind.COMMAND, spec.majorVersion(),
                    Optional.empty(), Optional.of(capabilities),
                    Optional.empty(), Optional.empty(),
                    Optional.of(new ConnectorOperationTypeContract(spec.inputType(), Optional.of(spec.outputType()))),
                    List.of()));
            }
        }

        ConnectorProviderArtifactDescriptor providerDescriptor = new ConnectorProviderArtifactDescriptor(
            new ConnectorProviderDescriptor(HttpConnector.PROVIDER_ID, new ConnectorProviderVersion(1, 0)),
            descriptors);
        ConnectorProviderManifestCatalog manifests = new ConnectorProviderManifestCatalog(
            List.of(new ConnectorProviderManifest(1, List.of(providerDescriptor))));

        return new HttpConnector(
            new HttpOperationCatalog(pins),
            new HttpOperationBindingCatalog(bindings),
            manifests,
            ProofHttpConnectorFactory.class.getClassLoader());
    }

    private static HttpWireSchema fieldSchema(HttpWireSchema objectSchema, String fieldName) {
        JsonNode field = objectSchema.node().path("properties").get(fieldName);
        if (field == null || field.isNull() || field.isMissingNode()) {
            return DEFAULT_PARAM_SCHEMA;
        }
        try {
            return new HttpWireSchema(PipelineJson.mapper().writeValueAsString(field));
        } catch (Exception e) {
            return DEFAULT_PARAM_SCHEMA;
        }
    }

    private static boolean isRequired(HttpWireSchema objectSchema, String fieldName) {
        JsonNode req = objectSchema.node().path("required");
        if (req.isArray()) {
            for (JsonNode item : req) {
                if (fieldName.equals(item.asText())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String sha256Hex(String text) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
