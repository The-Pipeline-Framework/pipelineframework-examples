package org.pipelineframework.awsconformance.faults;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ActionTransportFaultProxyTest {
    private static final String TARGET = "arn:aws:lambda:us-east-2:123456789012:function:production-action:7";
    private static final byte[] REQUEST = "{\"operation\":\"SUBMIT\", \"tenantId\":\"case-3\",\"inputJson\":\"opaque\"}".getBytes(StandardCharsets.UTF_8);

    @Test
    void forwardsExactRequestBytesAndReturnsExactProductionResponse() throws Exception {
        var lambda = mock(LambdaClient.class);
        var dynamo = mock(DynamoDbClient.class);
        when(dynamo.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        byte[] expected = "{\"duplicate\":false,\"opaque\":123}".getBytes(StandardCharsets.UTF_8);
        when(lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(SdkBytes.fromByteArray(expected)).build());
        var output = new ByteArrayOutputStream();
        new ActionTransportFaultProxy(lambda, dynamo, TARGET, "faults").handleRequest(
            new ByteArrayInputStream(REQUEST), output, mock(Context.class));
        var forwarded = ArgumentCaptor.forClass(InvokeRequest.class);
        verify(lambda).invoke(forwarded.capture());
        assertThat(forwarded.getValue().functionName()).isEqualTo(TARGET);
        assertThat(forwarded.getValue().payload().asByteArray()).isEqualTo(REQUEST);
        assertThat(output.toByteArray()).isEqualTo(expected);
    }

    @Test
    void beforeForwardFaultDoesNotCallProductionTarget() {
        var lambda = mock(LambdaClient.class);
        var dynamo = mock(DynamoDbClient.class);
        when(dynamo.getItem(any(GetItemRequest.class))).thenReturn(armed());
        assertThatThrownBy(() -> new ActionTransportFaultProxy(lambda, dynamo, TARGET, "faults")
            .handleRequest(new ByteArrayInputStream(REQUEST), new ByteArrayOutputStream(), mock(Context.class)))
            .isInstanceOf(IOException.class).hasMessageContaining("BEFORE_FORWARD");
        verifyNoInteractions(lambda);
        verify(dynamo).putItem(any(PutItemRequest.class));
    }

    @Test
    void lostResponseHappensAfterTheRealTargetAcceptedRequest() {
        var lambda = mock(LambdaClient.class);
        var dynamo = mock(DynamoDbClient.class);
        when(dynamo.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build(), armed());
        when(lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(SdkBytes.fromUtf8String("accepted")).build());
        var output = new ByteArrayOutputStream();
        assertThatThrownBy(() -> new ActionTransportFaultProxy(lambda, dynamo, TARGET, "faults")
            .handleRequest(new ByteArrayInputStream(REQUEST), output, mock(Context.class)))
            .isInstanceOf(IOException.class).hasMessageContaining("AFTER_RESPONSE");
        verify(lambda).invoke(any(InvokeRequest.class));
        assertThat(output.size()).isZero();
    }

    @Test
    void consumedFaultAllowsRetryToReachTheSamePinnedTarget() throws Exception {
        var lambda = mock(LambdaClient.class);
        var dynamo = mock(DynamoDbClient.class);
        when(dynamo.getItem(any(GetItemRequest.class))).thenReturn(armed());
        when(dynamo.putItem(any(PutItemRequest.class))).thenThrow(ConditionalCheckFailedException.builder().message("claimed").build());
        when(lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(SdkBytes.fromUtf8String("accepted")).build());
        var output = new ByteArrayOutputStream();
        new ActionTransportFaultProxy(lambda, dynamo, TARGET, "faults").handleRequest(
            new ByteArrayInputStream(REQUEST), output, mock(Context.class));
        assertThat(output.toString(StandardCharsets.UTF_8)).isEqualTo("accepted");
    }

    @Test
    void rejectsFloatingAliasOrUnqualifiedTarget() {
        for (String target : new String[] {TARGET.replace(":7", ":live"), TARGET.replace(":7", "")}) {
            assertThatThrownBy(() -> new ActionTransportFaultProxy(mock(LambdaClient.class), mock(DynamoDbClient.class), target, "faults"))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private GetItemResponse armed() {
        return GetItemResponse.builder().item(Map.of("fault_id", AttributeValue.fromS("armed"))).build();
    }
}
