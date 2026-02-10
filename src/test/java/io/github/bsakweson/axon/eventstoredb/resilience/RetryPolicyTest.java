package io.github.bsakweson.axon.eventstoredb.resilience;

import org.axonframework.common.AxonConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void shouldCreateDefaultPolicy() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();

        assertThat(policy.getMaxRetries()).isEqualTo(3);
        assertThat(policy.getInitialBackoffMs()).isEqualTo(100);
        assertThat(policy.getMaxBackoffMs()).isEqualTo(5000);
        assertThat(policy.getMultiplier()).isEqualTo(2.0);
        assertThat(policy.isEnabled()).isTrue();
    }

    @Test
    void shouldCreateNoRetryPolicy() {
        RetryPolicy policy = RetryPolicy.noRetry();

        assertThat(policy.getMaxRetries()).isZero();
        assertThat(policy.isEnabled()).isFalse();
    }

    @Test
    void shouldBuildCustomPolicy() {
        RetryPolicy policy = RetryPolicy.builder()
                .maxRetries(5)
                .initialBackoffMs(200)
                .maxBackoffMs(10000)
                .multiplier(3.0)
                .build();

        assertThat(policy.getMaxRetries()).isEqualTo(5);
        assertThat(policy.getInitialBackoffMs()).isEqualTo(200);
        assertThat(policy.getMaxBackoffMs()).isEqualTo(10000);
        assertThat(policy.getMultiplier()).isEqualTo(3.0);
        assertThat(policy.isEnabled()).isTrue();
    }

    @Test
    void shouldRejectNegativeMaxRetries() {
        assertThatThrownBy(() -> RetryPolicy.builder().maxRetries(-1))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("maxRetries");
    }

    @Test
    void shouldRejectNegativeInitialBackoff() {
        assertThatThrownBy(() -> RetryPolicy.builder().initialBackoffMs(-1))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("initialBackoffMs");
    }

    @Test
    void shouldRejectNegativeMaxBackoff() {
        assertThatThrownBy(() -> RetryPolicy.builder().maxBackoffMs(-1))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("maxBackoffMs");
    }

    @Test
    void shouldRejectMultiplierLessThanOne() {
        assertThatThrownBy(() -> RetryPolicy.builder().multiplier(0.5))
                .isInstanceOf(AxonConfigurationException.class)
                .hasMessageContaining("multiplier");
    }

    @Test
    void shouldAllowZeroRetries() {
        RetryPolicy policy = RetryPolicy.builder().maxRetries(0).build();
        assertThat(policy.isEnabled()).isFalse();
    }

    @Test
    void shouldHaveMeaningfulToString() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        String str = policy.toString();
        assertThat(str).contains("maxRetries=3");
        assertThat(str).contains("initialBackoffMs=100");
        assertThat(str).contains("maxBackoffMs=5000");
        assertThat(str).contains("multiplier=2.0");
    }
}
