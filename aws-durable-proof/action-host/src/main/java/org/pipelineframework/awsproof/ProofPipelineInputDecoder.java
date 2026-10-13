package org.pipelineframework.awsproof;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.aws.durable.AwsDurableInputDecoder;

/** Decoder for the legacy hand-authored proof; production conformance uses its generated decoder. */
@ApplicationScoped
final class ProofPipelineInputDecoder implements AwsDurableInputDecoder {
    private final ObjectMapper mapper;

    @Inject
    ProofPipelineInputDecoder(ObjectMapper mapper) {
        this.mapper = java.util.Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public ProofPipelineInput decode(String inputJson) {
        try {
            return java.util.Optional.ofNullable(mapper.readValue(inputJson, ProofPipelineInput.class))
                .orElseThrow(() -> new IllegalArgumentException("inputJson must contain a valid proof pipeline input"));
        } catch (JsonProcessingException malformed) {
            throw new IllegalArgumentException("inputJson must contain a valid proof pipeline input", malformed);
        }
    }
}
