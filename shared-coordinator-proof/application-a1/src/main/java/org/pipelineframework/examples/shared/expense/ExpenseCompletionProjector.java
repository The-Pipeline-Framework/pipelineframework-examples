package org.pipelineframework.examples.shared.expense;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.awaitable.AwaitCompletionProjector;
import org.pipelineframework.awaitable.AwaitCompletionMetadata;
/** Joins the authenticated decision to the retained pending request without external effects. */
@ApplicationScoped
public final class ExpenseCompletionProjector implements AwaitCompletionProjector<Pending, ApprovalReply, Approved> {
 public Approved project(Pending request, ApprovalReply reply, AwaitCompletionMetadata metadata) {
  return new Approved(request.requestId() + ":projected-A1", reply.approved());
 }
}
