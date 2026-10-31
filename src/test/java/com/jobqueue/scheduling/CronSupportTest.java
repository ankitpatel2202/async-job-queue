package com.jobqueue.scheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

class CronSupportTest {

    @Test
    void acceptsFiveFieldCron() {
        Instant next = CronSupport.next(CronSupport.parse("*/15 * * * *"), ZoneId.of("UTC"),
                Instant.parse("2026-11-01T10:07:30Z"));
        assertThat(next).isEqualTo(Instant.parse("2026-11-01T10:15:00Z"));
    }

    @Test
    void acceptsSixFieldCronWithSeconds() {
        Instant next = CronSupport.next(CronSupport.parse("*/10 * * * * *"), ZoneId.of("UTC"),
                Instant.parse("2026-11-01T10:00:01Z"));
        assertThat(next).isEqualTo(Instant.parse("2026-11-01T10:00:10Z"));
    }

    @Test
    void evaluatesInTheConfiguredZone() {
        // 02:00 in Kolkata (UTC+05:30) is 20:30 UTC the previous day
        Instant next = CronSupport.next(CronSupport.parse("0 2 * * *"), CronSupport.zone("Asia/Kolkata"),
                Instant.parse("2026-11-01T00:00:00Z"));
        assertThat(next).isEqualTo(Instant.parse("2026-11-01T20:30:00Z"));
    }

    @Test
    void rejectsGarbage() {
        assertThatThrownBy(() -> CronSupport.parse("every tuesday")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronSupport.parse(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronSupport.zone("Mars/Olympus")).isInstanceOf(IllegalArgumentException.class);
    }
}
