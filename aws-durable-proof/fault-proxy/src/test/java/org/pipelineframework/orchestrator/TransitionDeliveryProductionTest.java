package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashSet;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler;
import org.pipelineframework.awsconformance.faults.TransitionDeliveryFaultHandler;
import org.pipelineframework.config.pipeline.PipelineJson;
import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class TransitionDeliveryProductionTest {
    @Test
    void signedDuplicateCrossesBothCutsButNonceReplayNeverExecutesOrPublishesTwice() throws Exception {
        check(true);
    }

    @Test
    void authenticationRejectionAckIsNotResponsePublication() throws Exception {
        check(false);
    }

    private void check(boolean authentic) throws Exception {
        var service = mock(PipelineExecutionService.class);
        var sqs = mock(SqsClient.class);
        var config = mock(PipelineOrchestratorConfig.class);
        var worker = mock(PipelineOrchestratorConfig.SqsWorkerConfig.class);
        when(config.workerSqs()).thenReturn(worker);
        when(worker.responseQueueUrl()).thenReturn(Optional.of("https://sqs.local/response"));
        when(worker.requestTimeout()).thenReturn(Duration.ofSeconds(1));
        when(worker.signatureTolerance()).thenReturn(Duration.ofMinutes(5));
        when(worker.sharedSecret()).thenReturn(Optional.of("secret"));
        when(worker.sharedSecretRef()).thenReturn(Optional.empty());
        var codec = new JsonTransitionPayloadCodec();
        var command = new TransitionWorkerCommand("tenant-7", "execution-7", 0, 0, ExecutionResultShape.SINGLE,
            0L, "transition-7", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"));
        var envelope = TransitionCommandEnvelope.from(command, "pipeline-7", "release-7", "transition-7", codec.encode(command.inputPayload()));
        when(service.executePortableTransition(envelope)).thenReturn(Uni.createFrom().item(TransitionResultEnvelope.completed(codec, List.of("ok"))));
        when(sqs.sendMessage(any(SendMessageRequest.class))).thenReturn(SendMessageResponse.builder().messageId("published").build());
        var action = new SqsTransitionWorkerAction(config, service, new LocalControlPlaneSecretResolver(), sqs);
        var handler = new AwsDurableTransitionWorkerHandler();
        var field = AwsDurableTransitionWorkerHandler.class.getDeclaredField("action");
        field.setAccessible(true);
        field.set(handler, action);
        String encoded = PipelineJson.mapper().writeValueAsString(envelope);
        String timestamp = Instant.now().toString();
        String signature = authentic ? TransitionWorkerSignature.sign("secret", SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
            SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH, timestamp, "nonce-7", SqsTransitionWorkerProtocol.signedBytes("request-7", encoded)) : "invalid";
        String body = PipelineJson.mapper().writeValueAsString(new SqsTransitionWorkerRequest("request-7",
            SqsTransitionWorkerProtocol.PROTOCOL_VERSION, SqsTransitionWorkerProtocol.PAYLOAD_ENCODING, encoded, timestamp, "nonce-7", signature));
        var dynamo = mock(DynamoDbClient.class);
        when(dynamo.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(Map.of("fault_id", AttributeValue.fromS("armed"))).build());
        var claims = new HashSet<String>();
        when(dynamo.putItem(any(PutItemRequest.class))).thenAnswer(invocation -> {
            var put = invocation.getArgument(0, PutItemRequest.class);
            if (!claims.add(put.item().get("fault_id").s())) {
                throw ConditionalCheckFailedException.builder().message("consumed").build();
            }
            assertThat(put.item().get("request_id").s()).isEqualTo("request-7");
            assertThat(put.item().get("delegate").s()).isEqualTo(AwsDurableTransitionWorkerHandler.class.getName());
            return PutItemResponse.builder().build();
        });
        var wrapper = new TransitionDeliveryFaultHandler(handler, dynamo, "faults");
        var message = new SQSEvent.SQSMessage();
        message.setMessageId("original"); message.setBody(body);
        var event = new SQSEvent(); event.setRecords(List.of(message));
        var context = mock(com.amazonaws.services.lambda.runtime.Context.class);
        assertThat(wrapper.handleRequest(event, context).getBatchItemFailures()).hasSize(1);
        verifyNoInteractions(service, sqs); // before delegate and before nonce admission
        message.setMessageId("duplicate-1");
        assertThat(wrapper.handleRequest(event, context).getBatchItemFailures()).hasSize(1);
        if (authentic) {
            var sent = org.mockito.ArgumentCaptor.forClass(SendMessageRequest.class);
            verify(sqs).sendMessage(sent.capture()); // actual production publication precedes lost ACK
            var response = PipelineJson.mapper().readValue(sent.getValue().messageBody(), SqsTransitionWorkerResponse.class);
            assertThat(response.requestId()).isEqualTo("request-7");
            assertThat(TransitionWorkerSignature.matches(response.signature(), TransitionWorkerSignature.sign("secret",
                SqsTransitionWorkerProtocol.SIGNATURE_METHOD, SqsTransitionWorkerProtocol.RESPONSE_SIGNATURE_PATH,
                response.timestamp(), response.nonce(), SqsTransitionWorkerProtocol.signedBytes(response.requestId(), response.resultEnvelope())))).isTrue();
        } else { verifyNoInteractions(service, sqs); }
        message.setMessageId("duplicate-2");
        assertThat(wrapper.handleRequest(event, context).getBatchItemFailures()).isEmpty();
        verify(service, times(authentic ? 1 : 0)).executePortableTransition(any());
        verify(sqs, times(authentic ? 1 : 0)).sendMessage(any(SendMessageRequest.class));
    }
}
