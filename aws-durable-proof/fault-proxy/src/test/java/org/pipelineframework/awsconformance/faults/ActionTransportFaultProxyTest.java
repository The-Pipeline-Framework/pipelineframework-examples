package org.pipelineframework.awsconformance.faults;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.security.MessageDigest;
import java.util.HexFormat;

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

    @Test
    void registrationHoldSurvivesRetriesAndReleasesOnlyAtTheFixtureMarker() throws Exception {
        var lambda = mock(LambdaClient.class);
        var dynamo = mock(DynamoDbClient.class);
        var released = new AtomicBoolean(false);
        String armId = "tenant-held#REGISTER_CALLBACK#BEFORE_FORWARD";
        byte[] request = "{\"operation\":\"REGISTER_CALLBACK\",\"tenantId\":\"tenant-held\",\"executionId\":\"native-original\",\"generation\":1}".getBytes(StandardCharsets.UTF_8);
        when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(invocation -> {
            String key = invocation.<GetItemRequest>getArgument(0).key().get("fault_id").s();
            if (key.equals(armId)) {
                return GetItemResponse.builder().item(Map.of("fault_id", AttributeValue.fromS(armId), "mode", AttributeValue.fromS("HOLD"))).build();
            }
            if (key.equals(armId + "#RELEASE") && released.get()) {
                return GetItemResponse.builder().item(Map.of("fault_id", AttributeValue.fromS(key))).build();
            }
            return GetItemResponse.builder().build();
        });
        when(dynamo.putItem(any(PutItemRequest.class)))
            .thenReturn(software.amazon.awssdk.services.dynamodb.model.PutItemResponse.builder().build())
            .thenThrow(ConditionalCheckFailedException.builder().message("first attempt already claimed").build());
        when(lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(SdkBytes.fromUtf8String("accepted")).build());
        var proxy = new ActionTransportFaultProxy(lambda, dynamo, TARGET, "faults");
        for (int retry = 0; retry < 3; retry++) {
            assertThatThrownBy(() -> proxy.handleRequest(new ByteArrayInputStream(request), new ByteArrayOutputStream(), mock(Context.class)))
                .isInstanceOf(IOException.class).hasMessageContaining("Held external REGISTER_CALLBACK");
        }
        verifyNoInteractions(lambda);
        var claims = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamo, times(3)).putItem(claims.capture());
        var claim = claims.getAllValues().getFirst().item();
        assertThat(claim.get("tenant_id").s()).isEqualTo("tenant-held");
        assertThat(claim.get("execution_id").s()).isEqualTo("native-original");
        assertThat(claim.get("generation").n()).isEqualTo("1");
        assertThat(claim.get("request_json").s()).isEqualTo(new String(request, StandardCharsets.UTF_8));
        assertThat(claim.get("request_sha256").s()).isEqualTo(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(request)));
        released.set(true);
        var output = new ByteArrayOutputStream();
        proxy.handleRequest(new ByteArrayInputStream(request), output, mock(Context.class));
        var forwarded = ArgumentCaptor.forClass(InvokeRequest.class);
        verify(lambda).invoke(forwarded.capture());
        assertThat(forwarded.getValue().functionName()).isEqualTo(TARGET);
        assertThat(forwarded.getValue().payload().asByteArray()).isEqualTo(request);
        assertThat(output.toString(StandardCharsets.UTF_8)).isEqualTo("accepted");
    }

    @Test
    void holdIsInertForOtherOperationStageOrTenant() throws Exception {
        for (String armedId : new String[] {"case-3#SUBMIT#BEFORE_FORWARD", "case-3#REGISTER_CALLBACK#AFTER_RESPONSE", "other#REGISTER_CALLBACK#BEFORE_FORWARD"}) {
            var lambda = mock(LambdaClient.class);
            var dynamo = mock(DynamoDbClient.class);
            when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(invocation -> {
                String key = invocation.<GetItemRequest>getArgument(0).key().get("fault_id").s();
                return key.equals(armedId) ? GetItemResponse.builder().item(Map.of("fault_id", AttributeValue.fromS(key), "mode", AttributeValue.fromS("HOLD"))).build()
                    : GetItemResponse.builder().build();
            });
            when(lambda.invoke(any(InvokeRequest.class))).thenReturn(InvokeResponse.builder().payload(SdkBytes.fromUtf8String("accepted")).build());
            var output = new ByteArrayOutputStream();
            byte[] request = armedId.contains("SUBMIT") ? REQUEST : "{\"operation\":\"REGISTER_CALLBACK\",\"tenantId\":\"case-3\",\"executionId\":\"original\",\"generation\":1}".getBytes(StandardCharsets.UTF_8);
            new ActionTransportFaultProxy(lambda, dynamo, TARGET, "faults").handleRequest(new ByteArrayInputStream(request), output, mock(Context.class));
            assertThat(output.toString(StandardCharsets.UTF_8)).isEqualTo("accepted");
            verify(dynamo, never()).putItem(any(PutItemRequest.class));
        }
    }

    private GetItemResponse armed() {
        return GetItemResponse.builder().item(Map.of("fault_id", AttributeValue.fromS("armed"))).build();
    }
}
