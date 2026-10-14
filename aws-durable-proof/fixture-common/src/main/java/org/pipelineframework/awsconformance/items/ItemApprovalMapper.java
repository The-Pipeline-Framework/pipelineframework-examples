package org.pipelineframework.awsconformance.items;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.awsconformance.items.common.domain.Approval;
import org.pipelineframework.awsconformance.items.common.dto.ApprovalDto;
import org.pipelineframework.mapper.Mapper;

/** Pair-accurate mapping for the item application's private REST representation. */
@ApplicationScoped
public final class ItemApprovalMapper implements Mapper<Approval, ApprovalDto> {
    @Override
    public Approval fromExternal(ApprovalDto external) {
        return new Approval(external.request());
    }

    @Override
    public ApprovalDto toExternal(Approval domain) {
        return new ApprovalDto(domain.request());
    }
}
