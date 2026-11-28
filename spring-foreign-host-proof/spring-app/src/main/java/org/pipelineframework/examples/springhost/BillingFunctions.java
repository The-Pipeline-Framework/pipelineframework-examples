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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.pipelineframework.spring.host.TpfBusinessRejectionException;
import org.pipelineframework.spring.host.TpfFunction;
import org.pipelineframework.spring.host.TpfFunctionContext;
import org.pipelineframework.spring.host.TpfTransientException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Ordinary Spring {@code @Service} beans implementing {@code ONE_TO_ONE}, {@code ONE_TO_MANY},
 * {@code MANY_TO_MANY} full-duplex, and {@code MANY_TO_ONE} business steps across sync,
 * {@link Mono}, {@link Flux}, and {@link CompletionStage} method shapes.
 */
@Service
public class BillingFunctions {

    private final Set<String> seenTransientIds = ConcurrentHashMap.newKeySet();

    public record BillingOrder(
        String orderId,
        long sequenceNumber,
        BigDecimal unitAmount,
        int lineCount,
        boolean failFirstAttempt
    ) {
    }

    public record OrderLine(
        String orderId,
        int lineNumber,
        long sequenceNumber,
        BigDecimal lineAmount
    ) {
    }

    public record OrderSummary(
        String orderId,
        long sequenceNumber,
        int totalLines,
        BigDecimal grandTotal
    ) {
    }

    public record PaymentAwaitRequest(
        String paymentId,
        long sequenceNumber,
        BigDecimal amount,
        boolean failFirstAttempt
    ) {
    }

    public record PaymentSettled(
        String paymentId,
        long sequenceNumber,
        BigDecimal settledAmount
    ) {
    }

    /** Synchronous {@code ONE_TO_ONE} validation preserving exact 29-digit BigDecimal and odd int64. */
    @TpfFunction(stepId = "validate-order")
    public BillingOrder validateOrder(BillingOrder input, TpfFunctionContext ctx) {
        if (input.unitAmount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new TpfBusinessRejectionException("INVALID_ORDER", "Order amount must be positive");
        }
        if (input.failFirstAttempt() && seenTransientIds.add("order:" + input.orderId())) {
            throw new TpfTransientException("LEDGER_BUSY", "Simulated transient downstream contention");
        }
        return input;
    }

    /** Reactive {@code ONE_TO_MANY} streaming expansion into canonical line items. */
    @TpfFunction(stepId = "expand-lines")
    public Flux<OrderLine> expandLines(BillingOrder order) {
        return Flux.range(1, order.lineCount())
            .map(idx -> new OrderLine(
                order.orderId(),
                idx,
                order.sequenceNumber(),
                order.unitAmount()));
    }

    /** True full-duplex {@code MANY_TO_MANY} streaming enrichment without whole-stream buffering. */
    @TpfFunction(stepId = "enrich-lines", outputTypeId = "OrderLine")
    public Flux<OrderLine> enrichLines(Flux<OrderLine> lines) {
        return lines.map(line -> new OrderLine(
            line.orderId(),
            line.lineNumber(),
            line.sequenceNumber() + line.lineNumber(),
            line.lineAmount().add(new BigDecimal("0.000000001"))));
    }

    /** Reactive {@code MANY_TO_ONE} streaming reducer computing exact decimal totals. */
    @TpfFunction(stepId = "summarise-order", outputTypeId = "OrderSummary")
    public Mono<OrderSummary> summariseOrder(Flux<OrderLine> lines) {
        return lines.reduce(
            new OrderSummary("", 0L, 0, BigDecimal.ZERO),
            (acc, line) -> new OrderSummary(
                line.orderId(),
                Math.max(acc.sequenceNumber(), line.sequenceNumber()),
                acc.totalLines() + 1,
                acc.grandTotal().add(line.lineAmount())));
    }

    /** {@link CompletionStage} {@code ONE_TO_ONE} step used before durable Await boundaries. */
    @TpfFunction(stepId = "prepare-payment")
    public CompletionStage<PaymentAwaitRequest> preparePayment(PaymentAwaitRequest input, TpfFunctionContext ctx) {
        if (input.failFirstAttempt() && seenTransientIds.add("await:" + input.paymentId())) {
            return CompletableFuture.failedFuture(
                new TpfTransientException("GATEWAY_RETRY", "Transient upstream error before await"));
        }
        return CompletableFuture.completedFuture(new PaymentAwaitRequest(
            input.paymentId(),
            input.sequenceNumber() + 1L,
            input.amount(),
            false));
    }

    /** {@link Mono} {@code ONE_TO_ONE} step completing a resumed durable Await pipeline. */
    @TpfFunction(stepId = "finalise-payment")
    public Mono<PaymentSettled> finalisePayment(PaymentSettled input) {
        return Mono.just(new PaymentSettled(
            input.paymentId(),
            input.sequenceNumber(),
            input.settledAmount()));
    }
}
