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

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies the Spring Boot foreign host application module carries zero Quarkus, Vert.x,
 * Mutiny, CDI, or JAX-RS classes on its classpath.
 */
class SpringBillingHostDependencyGuardTest {

    @Test
    void springBootAppHasZeroQuarkusVertxOrMutinyDependencies() {
        ClassLoader loader = SpringBillingHostDependencyGuardTest.class.getClassLoader();
        for (String forbidden : List.of(
            "io.quarkus.runtime.Quarkus",
            "io.vertx.core.Vertx",
            "io.smallrye.mutiny.Uni",
            "jakarta.enterprise.context.ApplicationScoped",
            "jakarta.ws.rs.Path"
        )) {
            assertFalse(isPresent(loader, forbidden), "Forbidden dependency on Spring Boot app classpath: " + forbidden);
        }
    }

    private static boolean isPresent(ClassLoader loader, String name) {
        try {
            Class.forName(name, false, loader);
            return true;
        } catch (ClassNotFoundException expected) {
            return false;
        }
    }
}
