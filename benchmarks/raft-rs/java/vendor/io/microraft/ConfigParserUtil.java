/*
 * Copyright (c) 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft;

import java.math.BigDecimal;

final class ConfigParserUtil {

    private ConfigParserUtil() {
    }

    /**
     * Parses the given numeric value as an exact long.
     *
     * @throws IllegalArgumentException
     *             if the value is not numeric, fractional, or out of long range
     */
    static long getLong(Object value, String fieldName) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Config field '" + fieldName + "' must be a number: " + value);
        }

        try {
            return new BigDecimal(value.toString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Config field '" + fieldName + "' must be an integer in the long range: " + value, e);
        }
    }

    /**
     * Parses the given numeric value as an exact int.
     *
     * @throws IllegalArgumentException
     *             if the value is not numeric, fractional, or out of int range
     */
    static int getInt(Object value, String fieldName) {
        try {
            return Math.toIntExact(getLong(value, fieldName));
        } catch (ArithmeticException | IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Config field '" + fieldName + "' must be an integer in the int range: " + value, e);
        }
    }

}
