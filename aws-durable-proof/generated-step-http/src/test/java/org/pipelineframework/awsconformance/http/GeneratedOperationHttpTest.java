package org.pipelineframework.awsconformance.http;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import jakarta.inject.Inject;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import org.pipelineframework.awsconformance.items.common.dto.ApprovalDto;
import org.pipelineframework.awsconformance.items.pipeline.ProcessItemApprovalsRestClient;
import org.pipelineframework.awsconformance.items.pipeline.ProcessItemApprovalsRestClientStep;

import org.junit.jupiter.api.Test;
import io.quarkus.test.junit.QuarkusTest;

/** Exercises the actual compiler-generated REST resource and authored pure service. */
@QuarkusTest
class GeneratedOperationHttpTest {
    @TestHTTPResource
    URI baseUri;
    @Inject ProcessItemApprovalsRestClientStep generatedStep;
    @Test
    void bothGeneratedOperationsUseTheirTypedRestMapping() {
        for (String operation : new String[] {"first", "second"}) {
            given().contentType("application/json").body("{\"request\":\"real-" + operation + "\"}")
                .when().post("/api/v1/process-" + operation + "-approval/process")
                .then().statusCode(200).body("request", equalTo("real-" + operation));
        }
    }

    @Test
    void generatedStreamingOperationEmitsEveryTypedItemBeyondOneHundred() {
        given().contentType("application/json").accept("application/json").body("{\"request\":\"101\"}")
            .when().post("/api/v1/process-item-approvals/process")
            .then().statusCode(200).body("size()", equalTo(101))
            .body("[0].request", equalTo("approval-0"))
            .body("[100].request", equalTo("approval-100"));
    }

    @Test
    void matchingGeneratedClientDecodesEveryBufferedTypedItem() throws Exception {
        var client = QuarkusRestClientBuilder.newBuilder().baseUri(baseUri).build(ProcessItemApprovalsRestClient.class);
        try {
            var results = client.process("conformance-release-1", "", "", new ApprovalDto("101"))
                .await().atMost(Duration.ofSeconds(10));
            assertEquals(101, results.size());
            assertEquals(new ApprovalDto("approval-0"), results.getFirst());
            assertEquals(new ApprovalDto("approval-100"), results.getLast());
        } finally {
            ((AutoCloseable) client).close();
        }
    }

    @Test
    void actualGeneratedStepFlattensEveryItemThroughInvocationRuntime() throws Exception {
        withActualStepClient(() -> {
            var results = generatedStep.applyOneToMany(new ApprovalDto("101"))
                .collect().asList().await().atMost(Duration.ofSeconds(10));
            assertEquals(java.util.stream.IntStream.range(0, 101).mapToObj(index -> new ApprovalDto("approval-" + index)).toList(), results);
        });
    }

    @Test
    void emptyFiniteOutputStaysEmptyThroughTheGeneratedStep() throws Exception {
        given().contentType("application/json").accept("application/json").body("{\"request\":\"0\"}")
            .when().post("/api/v1/process-item-approvals/process")
            .then().statusCode(200).body("size()", equalTo(0));
        withActualStepClient(() -> assertEquals(java.util.List.of(), generatedStep.applyOneToMany(new ApprovalDto("0"))
            .collect().asList().await().atMost(Duration.ofSeconds(10))));
    }

    @Test
    void bufferedPrefixFailureFailsHttpAndGeneratedStepWithoutPartialSuccess() throws Exception {
        given().contentType("application/json").accept("application/json").body("{\"request\":\"prefix-failure\"}")
            .when().post("/api/v1/process-item-approvals/process").then().statusCode(500);
        withActualStepClient(() -> {
            var observed = new ArrayList<ApprovalDto>();
            var failure = assertThrows(RuntimeException.class, () -> generatedStep.applyOneToMany(new ApprovalDto("prefix-failure"))
                .onItem().invoke(observed::add).collect().asList().await().atMost(Duration.ofSeconds(10)));
            assertFalse(failure instanceof io.smallrye.mutiny.TimeoutException, "failure must be authoritative, not a test timeout");
            assertEquals(java.util.List.of(), observed);
        });
    }

    private void withActualStepClient(Runnable assertion) throws Exception {
        var client = QuarkusRestClientBuilder.newBuilder().baseUri(baseUri).build(ProcessItemApprovalsRestClient.class);
        var field = ProcessItemApprovalsRestClientStep.class.getDeclaredField("restClient");
        field.setAccessible(true);
        var original = field.get(generatedStep);
        field.set(generatedStep, client);
        try { assertion.run(); }
        finally { field.set(generatedStep, original); ((AutoCloseable) client).close(); }
    }
}
