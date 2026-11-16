package org.pipelineframework.examples.shared.expense;
public record Pending(String requestId, long amount, String revision) {}
