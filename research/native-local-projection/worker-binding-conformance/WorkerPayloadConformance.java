import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.nio.file.Path;
import java.util.stream.StreamSupport;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.PolyglotInt32PayloadBinding;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Runs shared payload cases through the packaged Java binding and legacy codec. */
final class WorkerPayloadConformance {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        var nodeContract = JSON.readValue(Path.of(args[1]).toFile(), PipelineContractDescriptor.class);
        var pythonContract = JSON.readValue(Path.of(args[2]).toFile(), PipelineContractDescriptor.class);
        if (!nodeContract.equals(pythonContract)) {
            throw new AssertionError("The Java coordinator would read different generated contracts");
        }
        var cases = JSON.readTree(Path.of(args[0]).toFile());
        ArrayNode results = JSON.createArrayNode();
        for (JsonNode test : cases) {
            results.addObject().put("id", test.required("id").textValue()).set("observed", observe(test));
        }
        System.out.println(JSON.writeValueAsString(results));
    }

    private static JsonNode observe(JsonNode test) {
        try {
            String[] typeIds = StreamSupport.stream(test.required("typeIds").spliterator(), false)
                .map(JsonNode::textValue)
                .toArray(String[]::new);
            var binding = new PolyglotInt32PayloadBinding(typeIds);
            return switch (test.required("operation").textValue()) {
                case "catalog" -> JSON.createObjectNode().put("accepted", true);
                case "encode" -> JSON.valueToTree(binding.encode(
                    test.required("typeId").textValue(), test.required("value").intValue()));
                case "decode" -> {
                    String typeId = test.required("typeId").textValue();
                    int value = binding.decode(typeId, envelope(test));
                    yield JSON.createObjectNode().put("value", value)
                        .put("canonical", binding.encode(typeId, value).payload());
                }
                case "legacy-encode" -> JSON.valueToTree(new JsonTransitionPayloadCodec().encode(
                    test.required("value").textValue()));
                case "legacy-decode" -> JSON.valueToTree(new JsonTransitionPayloadCodec().decode(envelope(test)));
                default -> throw new AssertionError("Unexpected operation in " + test);
            };
        } catch (IllegalArgumentException rejected) {
            return JSON.createObjectNode().put("error", rejected.getMessage());
        }
    }

    private static SerializedTransitionPayload envelope(JsonNode test) {
        var envelope = test.required("envelope");
        return new SerializedTransitionPayload(
            envelope.required("payloadTypeId").textValue(),
            envelope.required("payloadEncoding").textValue(),
            envelope.required("payload").textValue());
    }
}
