package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.VectorDto;
import com.example.ai.sdk.entity.Vector;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

import java.util.ArrayList;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * Embedding service that generates vector embeddings from text.
 * Implements Uni -> Uni cardinality (UnaryUnary).
 */
public class EmbeddingService implements ReactiveService<String, Vector> {

    /**
     * Create a Vector containing a deterministic 128-dimensional embedding for the given input text.
     *
     * <p>If {@code input} is {@code null} it is treated as the empty string. The returned Vector's id
     * uses the form {@code "embedding_<sha256>"} and its values are a deterministic 128-element
     * float list derived from the input text.
     *
     * @param input the text to embed; {@code null} is treated as an empty string
     * @return the Vector containing the computed id and the 128-dimensional embedding values
     */
    @Override
    public Uni<Vector> process(String input) {
        String safeInput = input == null ? "" : input;
        // Generate deterministic embedding based on text hash
        List<Float> embedding = generateDeterministicEmbedding(safeInput);
        String id = "embedding_" + sha256(safeInput);
        Vector vector = new Vector(id, embedding);

        return Uni.createFrom().item(vector);
    }

    private String sha256(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    /**
     * Produces a deterministic 128-dimensional embedding for the given text.
     *
     * @param text the input text used to deterministically generate the embedding (may be empty)
     * @return a List of 128 Float values representing the embedding for the provided text
     */
    private List<Float> generateDeterministicEmbedding(String text) {
        List<Float> embedding = new ArrayList<>(128);
        int seed = text.hashCode();

        // Generate 128-dimensional embedding
        for (int i = 0; i < 128; i++) {
            float value = (float) Math.sin(seed + i) * 0.1f;
            embedding.add(value);
        }

        return embedding;
    }

    /**
     * Create a VectorDto representation of the embedding derived from the given input.
     *
     * @param input the text to embed; null is treated as an empty string
     * @return a Uni containing a VectorDto whose id and values correspond to the generated embedding
     */
    public Uni<VectorDto> processDto(String input) {
        return process(input).map(v -> new VectorDto(v.id(), v.values()));
    }
}
