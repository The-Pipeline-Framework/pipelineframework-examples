package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.aws.durable.AwsDurableWorkItemHandler;
import org.pipelineframework.awsconformance.faults.WorkDeliveryFaultHandler;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real handler/action seam with substituted semantic service and storage; no live evidence. */
class WorkDeliveryProductionTest {
    private static final String BODY = "{\"tenantId\":\"tenant-4\",\"executionId\":\"execution-4\"}";

    @Test
    void beforeDelegateFailsWithoutAdmissionThenPassesUnchangedDuplicates() throws Exception {
        var fixture = fixture(Set.of("BEFORE_DELEGATE"), BODY);
        var context = mock(Context.class);
        assertThat(fixture.wrapper().handleRequest(event(BODY, "original"), context).getBatchItemFailures()).hasSize(1);
        verifyNoInteractions(fixture.service());
        assertThat(fixture.wrapper().handleRequest(event(BODY, "duplicate-1"), context).getBatchItemFailures()).isEmpty();
        assertThat(fixture.wrapper().handleRequest(event(BODY, "duplicate-2"), context).getBatchItemFailures()).isEmpty();
        verify(fixture.service(), times(2)).processExecutionWorkItem(new ExecutionWorkItem("tenant-4", "execution-4"));
        assertThat(fixture.claims()).containsExactly("tenant-4#WORK#BEFORE_DELEGATE#CLAIM");
    }

    @Test
    void committedActionPrecedesLostAckAndOneShotClaim() throws Exception {
        var fixture = fixture(Set.of("AFTER_ACK"), BODY);
        var context = mock(Context.class);
        assertThat(fixture.wrapper().handleRequest(event(BODY, "original"), context).getBatchItemFailures()).hasSize(1);
        verify(fixture.service()).processExecutionWorkItem(new ExecutionWorkItem("tenant-4", "execution-4"));
        assertThat(fixture.wrapper().handleRequest(event(BODY, "duplicate"), context).getBatchItemFailures()).isEmpty();
        verify(fixture.service(), times(2)).processExecutionWorkItem(new ExecutionWorkItem("tenant-4", "execution-4"));
        assertThat(fixture.claims()).containsExactly("tenant-4#WORK#AFTER_ACK#CLAIM");
    }

    @Test
    void productionRetryDoesNotClaimSuccessfulActionCut() throws Exception {
        var fixture = fixture(Set.of("AFTER_ACK"), BODY);
        when(fixture.service().processExecutionWorkItem(any())).thenReturn(Uni.createFrom().failure(new IllegalStateException("unavailable")));
        assertThat(fixture.wrapper().handleRequest(event(BODY, "original"), mock(Context.class)).getBatchItemFailures()).hasSize(1);
        assertThat(fixture.claims()).isEmpty();
        verifyNoInteractions(fixture.controlPlane());
    }

    @Test
    void concurrentNoOpAckCannotConsumeCutBeforeOtherDeliveryCommits() throws Exception {
        var fixture = fixture(Set.of("AFTER_ACK"), BODY);
        when(fixture.controlPlane().getExecutionSemanticCheckpoint("tenant-4", "execution-4"))
            .thenReturn(Uni.createFrom().item(new ExecutionSemanticCheckpoint("tenant-4", "execution-4",
                "pipeline", "contract", "release", ExecutionStatus.RUNNING)))
            .thenReturn(Uni.createFrom().item(new ExecutionSemanticCheckpoint("tenant-4", "execution-4",
                "pipeline", "contract", "release", ExecutionStatus.WAITING_EXTERNAL)));
        var context = mock(Context.class);
        assertThat(fixture.wrapper().handleRequest(event(BODY, "lease-losing-noop"), context).getBatchItemFailures()).isEmpty();
        assertThat(fixture.claims()).isEmpty();
        verify(fixture.controlPlane(), never()).getCurrentAwaitSemanticCheckpoint(anyString(), anyString());
        // Another delivery's actual action now commits before its read-only observation.
        assertThat(fixture.wrapper().handleRequest(event(BODY, "original-committed"), context).getBatchItemFailures()).hasSize(1);
        assertThat(fixture.claims()).containsExactly("tenant-4#WORK#AFTER_ACK#CLAIM");
    }

    @Test
    void queuedOrForeignReleaseOrMissingFirstAwaitCannotEstablishCommitCut() throws Exception {
        for (String cut : List.of("queued", "tenant", "execution", "pipeline", "contract", "release", "absent", "later")) {
            var fixture = fixture(Set.of("AFTER_ACK"), BODY);
            when(fixture.controlPlane().getExecutionSemanticCheckpoint(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(new ExecutionSemanticCheckpoint(
                    cut.equals("tenant") ? "other" : "tenant-4", cut.equals("execution") ? "other" : "execution-4",
                    cut.equals("pipeline") ? "other" : "pipeline", cut.equals("contract") ? "other" : "contract",
                    cut.equals("release") ? "other" : "release", cut.equals("queued") ? ExecutionStatus.QUEUED : ExecutionStatus.WAITING_EXTERNAL)));
            if (cut.equals("absent")) {
                when(fixture.controlPlane().getCurrentAwaitSemanticCheckpoint(anyString(), anyString())).thenReturn(Uni.createFrom().item(Optional.empty()));
            } else if (cut.equals("later")) {
                when(fixture.controlPlane().getCurrentAwaitSemanticCheckpoint(anyString(), anyString())).thenReturn(Uni.createFrom().item(Optional.of(
                    new AwaitSemanticCheckpoint("tenant-4", "execution-4", "second", "correlation", "unit", "SecondApproval",
                        AwaitInteractionStatus.WAITING, "pipeline", "contract", "release"))));
            }
            assertThat(fixture.wrapper().handleRequest(event(BODY, "noop"), mock(Context.class)).getBatchItemFailures()).isEmpty();
            assertThat(fixture.claims()).as(cut).isEmpty();
        }
    }

    @Test
    void malformedBodyUsesProductionDispositionWithoutFixtureStorage() throws Exception {
        var fixture = fixture(Set.of("AFTER_ACK"), BODY);
        assertThat(fixture.wrapper().handleRequest(event("not-json", "malformed"), mock(Context.class)).getBatchItemFailures()).isEmpty();
        verifyNoInteractions(fixture.service(), fixture.dynamo());
    }

    @Test
    void armCannotCutAnotherExecutionOrDifferentPacket() throws Exception {
        var fixture = fixture(Set.of("BEFORE_DELEGATE", "AFTER_ACK"), BODY);
        String other = "{\"tenantId\":\"tenant-4\",\"executionId\":\"other\"}";
        assertThat(fixture.wrapper().handleRequest(event(other, "other"), mock(Context.class)).getBatchItemFailures()).isEmpty();
        verify(fixture.service()).processExecutionWorkItem(new ExecutionWorkItem("tenant-4", "other"));
        assertThat(fixture.wrapper().handleRequest(event(BODY + " ", "different-body"), mock(Context.class)).getBatchItemFailures()).isEmpty();
        verify(fixture.service()).processExecutionWorkItem(new ExecutionWorkItem("tenant-4", "execution-4"));
        assertThat(fixture.claims()).isEmpty();
    }

    @Test
    void multiRecordBatchFailsAllWithoutDelegation() throws Exception {
        var fixture = fixture(Set.of("BEFORE_DELEGATE"), BODY);
        var event = event(BODY, "first");
        event.setRecords(List.of(event.getRecords().get(0), event(BODY, "second").getRecords().get(0)));
        assertThat(fixture.wrapper().handleRequest(event, mock(Context.class)).getBatchItemFailures())
            .extracting(failure -> failure.getItemIdentifier()).containsExactly("first", "second");
        verifyNoInteractions(fixture.service(), fixture.dynamo());
    }

    private Fixture fixture(Set<String> points, String body) throws Exception {
        var service = mock(PipelineExecutionService.class);
        when(service.processExecutionWorkItem(any())).thenReturn(Uni.createFrom().voidItem());
        var handler = new AwsDurableWorkItemHandler();
        var field = AwsDurableWorkItemHandler.class.getDeclaredField("action");
        field.setAccessible(true);
        field.set(handler, new SqsWorkItemAction(service));
        var dynamo = mock(DynamoDbClient.class);
        var controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getExecutionSemanticCheckpoint(anyString(), anyString())).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant-4", "execution-4", "pipeline", "contract", "release", ExecutionStatus.WAITING_EXTERNAL)));
        when(controlPlane.getCurrentAwaitSemanticCheckpoint(anyString(), anyString())).thenReturn(Uni.createFrom().item(Optional.of(
            new AwaitSemanticCheckpoint("tenant-4", "execution-4", "first", "correlation", "unit", "FirstApproval",
                AwaitInteractionStatus.WAITING, "pipeline", "contract", "release"))));
        var digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(invocation -> {
            var key = invocation.getArgument(0, GetItemRequest.class).key().get("fault_id").s();
            return GetItemResponse.builder().item(points.stream().anyMatch(key::endsWith)
                ? Map.of("fault_id", AttributeValue.fromS(key), "tenant_id", AttributeValue.fromS("tenant-4"),
                    "execution_id", AttributeValue.fromS("execution-4"), "body_sha256", AttributeValue.fromS(digest),
                    "work_item_type", AttributeValue.fromS(ExecutionWorkItem.class.getName()),
                    "pipeline_id", AttributeValue.fromS("pipeline"), "contract_version", AttributeValue.fromS("contract"),
                    "release_version", AttributeValue.fromS("release")) : Map.of()).build();
        });
        var claims = new HashSet<String>();
        when(dynamo.putItem(any(PutItemRequest.class))).thenAnswer(invocation -> {
            var put = invocation.getArgument(0, PutItemRequest.class);
            if (!claims.add(put.item().get("fault_id").s())) {
                throw ConditionalCheckFailedException.builder().message("consumed").build();
            }
            if (put.item().get("point").s().equals("BEFORE_DELEGATE")) { verifyNoInteractions(service); }
            else {
                verify(service, atLeastOnce()).processExecutionWorkItem(any());
                assertThat(put.item().get("execution_status").s()).isEqualTo("WAITING_EXTERNAL");
                assertThat(put.item().get("interaction_id").s()).isEqualTo("first");
                assertThat(put.item().get("commit_observation").s()).isEqualTo("PRODUCTION_CONTROL_PLANE_POINT_READS");
            }
            assertThat(put.conditionExpression()).isEqualTo("attribute_not_exists(fault_id)");
            assertThat(put.item().get("delegate").s()).isEqualTo(AwsDurableWorkItemHandler.class.getName());
            assertThat(put.item().get("body_sha256").s()).isEqualTo(digest);
            assertThat(put.item().get("execution_id").s()).isEqualTo("execution-4");
            return PutItemResponse.builder().build();
        });
        return new Fixture(new WorkDeliveryFaultHandler(handler, dynamo, "faults", controlPlane), service, dynamo, claims, controlPlane);
    }

    private SQSEvent event(String body, String id) {
        var message = new SQSEvent.SQSMessage();
        message.setBody(body); message.setMessageId(id);
        var event = new SQSEvent(); event.setRecords(List.of(message));
        return event;
    }

    private record Fixture(WorkDeliveryFaultHandler wrapper, PipelineExecutionService service,
                           DynamoDbClient dynamo, Set<String> claims, PipelineControlPlane controlPlane) { }
}
