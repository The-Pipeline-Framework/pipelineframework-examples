package org.pipelineframework.examples.shared.proof;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;

/** Local Function URL event bridge. The child JVM loads Lambda and Quarkus from the extracted ZIP. */
public final class LocalLambdaHttp {
    public static void main(String[] arguments) throws Exception {
        Class<?> handlerType = Class.forName("io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler");
        var handler = handlerType.getConstructor().newInstance();
        var invoke = handlerType.getMethod("handleRequest", InputStream.class, OutputStream.class,
            Class.forName("com.amazonaws.services.lambda.runtime.Context"));
        var json = new ObjectMapper();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", Integer.parseInt(arguments[0])), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", exchange -> {
            try (exchange) {
                var headers = new LinkedHashMap<String, String>();
                exchange.getRequestHeaders().forEach((key, values) ->
                    headers.put(key.toLowerCase(Locale.ROOT), String.join(",", values)));
                String path = exchange.getRequestURI().getRawPath();
                var event = Map.of(
                    "version", "2.0", "rawPath", path,
                    "rawQueryString", java.util.Objects.toString(exchange.getRequestURI().getRawQuery(), ""),
                    "headers", headers,
                    "requestContext", Map.of("http", Map.of("method", exchange.getRequestMethod(),
                        "path", path, "sourceIp", "127.0.0.1", "protocol", "HTTP/1.1")),
                    "body", Base64.getEncoder().encodeToString(exchange.getRequestBody().readAllBytes()),
                    "isBase64Encoded", true);
                var response = new ByteArrayOutputStream();
                invoke.invoke(handler, new ByteArrayInputStream(json.writeValueAsBytes(event)), response, null);
                var result = json.readTree(response.toByteArray());
                result.path("headers").properties().forEach(header ->
                    exchange.getResponseHeaders().set(header.getKey(), header.getValue().asText()));
                byte[] body = result.path("isBase64Encoded").asBoolean(false)
                    ? Base64.getDecoder().decode(result.path("body").asText())
                    : result.path("body").asText().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(result.path("statusCode").asInt(), body.length == 0 ? -1 : body.length);
                exchange.getResponseBody().write(body);
            } catch (Exception failure) {
                failure.printStackTrace(System.err);
            }
        });
        server.start();
    }
}
