package org.pipelineframework.examples.shared.parcel;
/** Parcel request after identity and positive weight validation. */
public record ValidatedRequest(String parcelId, int weight) {}
