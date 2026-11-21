/*
 * Copyright 2023-2026 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.labs.zdbdump.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

/**
 * Protects manual dump generation with stateless HTTP Basic authentication.
 * The dump listing, downloads and health endpoint are public.
 */
@Configuration(proxyBeanMethods = false)
@EnableWebSecurity
public class BasicConfiguration {

    private static final String ADMIN_ROLE = "ADMIN";

    private final String userName;
    private final String userPassword;
    private final boolean triggerEnabled;

    /**
     * Configures the account that may trigger a dump run. A blank credential disables manual generation.
     *
     * @param userName configured username for HTTP Basic authentication
     * @param userPassword configured password for HTTP Basic authentication
     */
    public BasicConfiguration(
            @Value("${spring.security.user.name}") String userName,
            @Value("${spring.security.user.password}") String userPassword) {
        this.userName = userName;
        this.userPassword = userPassword;
        this.triggerEnabled = !userName.isBlank() && !userPassword.isBlank();
    }

    /**
     * Creates the trigger account only when both credentials are configured.
     *
     * @param passwordEncoder encoder used for the stored password
     * @return in-memory user store for the manual trigger
     */
    @Bean
    public InMemoryUserDetailsManager userDetailsService(PasswordEncoder passwordEncoder) {
        if (!triggerEnabled) {
            return new InMemoryUserDetailsManager();
        }
        final UserDetails admin = User.withUsername(userName)
                .password(passwordEncoder.encode(userPassword))
                .roles(ADMIN_ROLE)
                .build();

        return new InMemoryUserDetailsManager(admin);
    }

    /**
     * Requires the trigger header and administrator role for manual generation requests.
     * The filter chain handles authentication for each request without creating a session.
     *
     * @param http Spring Security builder for the servlet application
     * @return filter chain for public downloads and the authenticated trigger
     * @throws Exception if Spring Security cannot build the filter chain
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        final RequestMatcher trigger = PathPatternRequestMatcher.withDefaults().matcher(HttpMethod.POST, "/createdump");
        // The trigger is exempt from CSRF token checks and requires an explicit header. A cross-origin
        // browser request can send the header only after a successful CORS preflight.
        final RequestMatcher confirmedTrigger = request -> triggerEnabled && trigger.matches(request)
                && "true".equals(request.getHeader("X-ZDBDUMP-TRIGGER"));
        return http
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .csrf(csrf -> csrf.ignoringRequestMatchers(trigger))
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers(confirmedTrigger).hasRole(ADMIN_ROLE)
                        .requestMatchers(trigger).denyAll()
                        .anyRequest().permitAll())
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    /**
     * Supplies Spring Security's delegating password encoder.
     *
     * @return encoder used to hash and verify the manual trigger password
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }
}
