package org.pipelineframework.awsconformance;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.awsconformance.common.domain.Approval;
import org.pipelineframework.awsconformance.common.dto.ApprovalDto;
import org.pipelineframework.mapper.Mapper;

/** Explicit pair-accurate representation mapping; no application behavior lives here. */
@ApplicationScoped
public final class ApprovalMapper implements Mapper<Approval, ApprovalDto> {
    @Override
    public Approval fromExternal(ApprovalDto external) {
        return new Approval(external.request());
    }

    @Override
    public ApprovalDto toExternal(Approval domain) {
        return new ApprovalDto(domain.request());
    }
}
