package org.apache.flink.statefun.flink.core.httpfn;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import java.time.Duration;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DefaultHttpRequestReplyClientSpecTest {

  @Test
  public void jsonSerDe() throws JsonProcessingException {
    final Duration callTimeout = Duration.ofDays(1L);
    final Duration connectTimeout = Duration.ofNanos(2L);
    final Duration readTimeout = Duration.ofSeconds(3L);
    final Duration writeTimeout = Duration.ofMillis(4L);

    final DefaultHttpRequestReplyClientSpec.Timeouts timeouts =
        new DefaultHttpRequestReplyClientSpec.Timeouts();
    timeouts.setCallTimeout(callTimeout);
    timeouts.setConnectTimeout(connectTimeout);
    timeouts.setReadTimeout(readTimeout);
    timeouts.setWriteTimeout(writeTimeout);

    final String trustCaCerts = "classpath:certs/caCerts";
    final String clientCerts = "file:certs/clientCerts";
    final String clientKey = "/tmp/clients.pem";

    final DefaultHttpRequestReplyClientSpec defaultHttpRequestReplyClientSpec =
        new DefaultHttpRequestReplyClientSpec();
    defaultHttpRequestReplyClientSpec.setTimeouts(timeouts);
    defaultHttpRequestReplyClientSpec.setTrustCaCerts(trustCaCerts);
    defaultHttpRequestReplyClientSpec.setClientCerts(clientCerts);
    defaultHttpRequestReplyClientSpec.setClientKey(clientKey);

    final ObjectMapper objectMapper = StateFunObjectMapper.create();
    final ObjectNode json = defaultHttpRequestReplyClientSpec.toJson(objectMapper);

    final DefaultHttpRequestReplyClientSpec deserializedHttpRequestReplyClientSpec =
        DefaultHttpRequestReplyClientSpec.fromJson(objectMapper, json);

    assertThat(deserializedHttpRequestReplyClientSpec.getTimeouts(), equalTimeouts(timeouts));
    assertThat(deserializedHttpRequestReplyClientSpec.getTrustCaCerts(), is(trustCaCerts));
    assertThat(deserializedHttpRequestReplyClientSpec.getClientCerts(), is(clientCerts));
    assertThat(deserializedHttpRequestReplyClientSpec.getClientKey(), is(clientKey));
  }

  private static TypeSafeDiagnosingMatcher<DefaultHttpRequestReplyClientSpec.Timeouts>
      equalTimeouts(DefaultHttpRequestReplyClientSpec.Timeouts timeouts) {
    return new TimeoutsEqualityMatcher(timeouts);
  }

  private static class TimeoutsEqualityMatcher
      extends TypeSafeDiagnosingMatcher<DefaultHttpRequestReplyClientSpec.Timeouts> {
    private final DefaultHttpRequestReplyClientSpec.Timeouts expected;

    private TimeoutsEqualityMatcher(DefaultHttpRequestReplyClientSpec.Timeouts timeouts) {
      this.expected = timeouts;
    }

    @Override
    protected boolean matchesSafely(
        DefaultHttpRequestReplyClientSpec.Timeouts timeouts, Description description) {

      return Stream.of(isMatching(expected.getCallTimeout(), timeouts.getCallTimeout(), description),
              isMatching(expected.getReadTimeout(), timeouts.getReadTimeout(), description),
              isMatching(expected.getWriteTimeout(), timeouts.getWriteTimeout(), description),
              isMatching(expected.getConnectTimeout(), timeouts.getConnectTimeout(), description))
          .allMatch(partialResult -> partialResult);
    }

    private boolean isMatching(Duration expected, Duration actual, Description description) {
      if (!actual.equals(expected)) {
        description.appendText("expected ").appendValue(expected).appendText(" found ").appendValue(actual);
        return false;
      }
      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Matches equality of Timeouts");
    }
  }
}
