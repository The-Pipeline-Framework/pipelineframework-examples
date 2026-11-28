/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.examples.springhost;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pipelineframework.spring.host.TpfCommandConnector;
import org.pipelineframework.spring.host.TpfCommandContext;
import org.pipelineframework.spring.host.TpfCommandReconciler;
import org.pipelineframework.spring.host.TpfCommandResult;
import org.pipelineframework.spring.host.TpfConnectorContext;
import org.pipelineframework.spring.host.TpfQueryConnector;
import org.pipelineframework.spring.host.TpfQueryResult;
import org.pipelineframework.spring.host.TpfReconcileContext;
import org.pipelineframework.spring.host.TpfReconcileResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Spring {@code @Service} implementing Tier-2 Custom Query, Command, and Command Reconciler
 * connectors under ADR-0092/ADR-0094.
 */
@Service
public class BillingConnectors {

    private final Map<String, ChargeReceipt> ledgerByIdempotencyKey = new ConcurrentHashMap<>();

    public record AccountLookup(String accountId) {
    }

    public record AccountProfile(String accountId, String tier, String region) {
    }

    public record ChargeCommand(String accountId, BigDecimal amount, boolean simulateAmbiguous) {
    }

    public record ChargeReceipt(String chargeId, String accountId, BigDecimal chargedAmount, String idempotencyKey) {
    }

    @TpfQueryConnector(operation = "billing.lookup")
    public TpfQueryResult<AccountProfile> lookupAccount(AccountLookup input, TpfConnectorContext ctx) {
        String apiKey = ctx.secret("BILLING_API_KEY").orElse("");
        if (!"authorized-billing-key".equals(apiKey)) {
            return TpfQueryResult.authRequired("missing-billing-api-key");
        }
        if ("unknown".equals(input.accountId())) {
            return TpfQueryResult.notFound("account-not-found");
        }
        return TpfQueryResult.found(new AccountProfile(
            input.accountId(), "enterprise", ctx.scope().orElse("eu-west")));
    }

    @TpfCommandConnector(operation = "billing.charge")
    public Mono<TpfCommandResult<ChargeReceipt>> chargeAccount(ChargeCommand input, TpfCommandContext ctx) {
        String apiKey = ctx.secret("BILLING_API_KEY").orElse("");
        if (!"authorized-billing-key".equals(apiKey)) {
            return Mono.just(TpfCommandResult.terminalFailure("unauthorized-billing-key"));
        }
        ChargeReceipt receipt = ledgerByIdempotencyKey.computeIfAbsent(
            ctx.providerIdempotencyKey(),
            key -> new ChargeReceipt("chg-" + input.accountId(), input.accountId(), input.amount(), key));
        if (input.simulateAmbiguous()) {
            // Provider applied the charge before the HTTP response was lost — reconciler will confirm it!
            return Mono.just(TpfCommandResult.ambiguous("simulated-timeout-after-apply"));
        }
        return Mono.just(TpfCommandResult.succeeded(receipt, "rcpt-" + ctx.commandId()));
    }

    @TpfCommandReconciler(operation = "billing.charge")
    public TpfReconcileResult<ChargeReceipt> reconcileCharge(TpfReconcileContext ctx) {
        ChargeReceipt applied = ledgerByIdempotencyKey.get(ctx.providerIdempotencyKey());
        if (applied == null) {
            return TpfReconcileResult.unresolved("charge-not-found-in-ledger");
        }
        return TpfReconcileResult.confirmedSucceeded(applied, "rcpt-reconciled-" + ctx.commandId());
    }
}
