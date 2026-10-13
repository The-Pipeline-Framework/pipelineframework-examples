package org.pipelineframework.awsconformance.compute;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import jakarta.inject.Inject;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awsconformance.items.common.dto.ApprovalDto;
import org.pipelineframework.awsconformance.compute.pipeline.ProcessComputeItemsRestClient;
import org.pipelineframework.awsconformance.compute.pipeline.ProcessComputeItemsRestClientStep;

/** Real generated COMPUTE server/client control for the FUNCTION buffering repair. */
@QuarkusTest
class GeneratedComputeHttpTest {
    @TestHTTPResource URI baseUri;
    @Inject ProcessComputeItemsRestClientStep generatedStep;

    @Test
    void generatedComputeResourceStillUsesServerSentEvents() {
        given().contentType("application/json").accept("text/event-stream").body("{\"request\":\"101\"}")
            .when().post("/api/v1/process-compute-items/process")
            .then().statusCode(200).contentType(containsString("text/event-stream"))
            .body(containsString("approval-0"), containsString("approval-100"));
    }

    @Test
    void generatedComputeClientStillConsumesEveryTypedStreamItem() throws Exception {
        var client = QuarkusRestClientBuilder.newBuilder().baseUri(baseUri).build(ProcessComputeItemsRestClient.class);
        try {
            var results = client.process("control-release-1", "", "", new ApprovalDto("101"))
                .collect().asList().await().atMost(Duration.ofSeconds(10));
            assertEquals(101, results.size());
            assertEquals(new ApprovalDto("approval-0"), results.getFirst());
            assertEquals(new ApprovalDto("approval-100"), results.getLast());
        } finally {
            ((AutoCloseable) client).close();
        }
    }

    @Test
    void actualGeneratedComputeStepPreservesEveryItemThroughInvocationRuntime() throws Exception {
        withActualStepClient(() -> {
            var results = generatedStep.applyOneToMany(new ApprovalDto("101"))
                .collect().asList().await().atMost(Duration.ofSeconds(10));
            assertEquals(java.util.stream.IntStream.range(0, 101).mapToObj(index -> new ApprovalDto("approval-" + index)).toList(), results);
        });
    }

    @Test
    void emptyStreamStaysEmptyThroughTheGeneratedComputeStep() throws Exception {
        withActualStepClient(() -> assertEquals(java.util.List.of(), generatedStep.applyOneToMany(new ApprovalDto("0"))
            .collect().asList().await().atMost(Duration.ofSeconds(10))));
    }

    @Test
    void streamingPrefixFailureRemainsATerminalFailure() throws Exception {
        withActualStepClient(() -> {
            var observed = new ArrayList<ApprovalDto>();
            var failure = assertThrows(RuntimeException.class, () -> generatedStep.applyOneToMany(new ApprovalDto("prefix-failure"))
                .onItem().invoke(observed::add).collect().asList().await().atMost(Duration.ofSeconds(10)));
            assertFalse(failure instanceof io.smallrye.mutiny.TimeoutException, "failure must be authoritative, not a test timeout");
            org.junit.jupiter.api.Assertions.assertTrue(observed.isEmpty()
                || observed.equals(java.util.List.of(new ApprovalDto("approval-0"))),
                "an immediate stream failure may precede flushing its one valid prefix");
        });
    }

    private void withActualStepClient(Runnable assertion) throws Exception {
        var client = QuarkusRestClientBuilder.newBuilder().baseUri(baseUri).build(ProcessComputeItemsRestClient.class);
        var field = ProcessComputeItemsRestClientStep.class.getDeclaredField("restClient");
        field.setAccessible(true);
        var original = field.get(generatedStep);
        field.set(generatedStep, client);
        try { assertion.run(); }
        finally { field.set(generatedStep, original); ((AutoCloseable) client).close(); }
    }
}
