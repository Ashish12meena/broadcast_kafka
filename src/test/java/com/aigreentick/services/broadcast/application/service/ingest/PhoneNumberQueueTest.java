package com.aigreentick.services.broadcast.application.service.ingest;

import com.aigreentick.services.broadcast.domain.model.DispatchBatch;
import com.aigreentick.services.broadcast.domain.model.Recipient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PhoneNumberQueueTest {

    private static final String PHONE_NUMBER_ID = "123456789012345";

    @Test
    @DisplayName("a large campaign cannot starve a small one on the same number")
    void fairnessAcrossCampaigns() {
        // The failure this prevents: with one queue per phone number, a hundred-thousand recipient
        // campaign that arrives first drains completely before a five-hundred recipient campaign on
        // the same number sends anything at all.
        PhoneNumberQueue queue = new PhoneNumberQueue(PHONE_NUMBER_ID);
        queue.offer(batch(1L, 100));
        queue.offer(batch(2L, 4));

        List<PendingSend> drained = queue.drain(8);

        long fromLargeCampaign = drained.stream().filter(send -> isCampaign(send, 1L)).count();
        long fromSmallCampaign = drained.stream().filter(send -> isCampaign(send, 2L)).count();

        assertThat(drained).hasSize(8);
        assertThat(fromSmallCampaign).isEqualTo(4);
        assertThat(fromLargeCampaign).isEqualTo(4);
    }

    @Test
    @DisplayName("retries are served before untried work")
    void retriesGoFirst() {
        // A retry is older than anything in the batches and is usually a rate limit that has since
        // cleared. Leaving it behind a large campaign turns a transient failure into a stale one.
        PhoneNumberQueue queue = new PhoneNumberQueue(PHONE_NUMBER_ID);
        InFlightBatch original = batch(1L, 10);
        queue.offer(original);

        PendingSend retry = new PendingSend(original, new Recipient(999L, 999L, 999L, "{}"));
        queue.requeue(retry);

        List<PendingSend> drained = queue.drain(3);

        assertThat(drained.get(0).recipient().recipientId()).isEqualTo(999L);
    }

    @Test
    @DisplayName("a batch is complete only when every recipient is resolved")
    void batchCompletesOnlyOnce() {
        // The Kafka offset moves on this signal, and Kafka has no partial acknowledgement: moving it
        // early discards the recipients that were never sent.
        boolean[] completed = {false};
        InFlightBatch inFlight = new InFlightBatch(
                new DispatchBatch(1L, PHONE_NUMBER_ID, 10L, "token", recipients(3)),
                () -> completed[0] = true);

        assertThat(inFlight.recordResolved()).isFalse();
        assertThat(inFlight.recordResolved()).isFalse();
        assertThat(inFlight.recordResolved()).isTrue();

        inFlight.complete();
        inFlight.complete();

        assertThat(completed[0]).isTrue();
        assertThat(inFlight.isCompleted()).isTrue();
    }

    @Test
    void drainStopsWhenEmpty() {
        PhoneNumberQueue queue = new PhoneNumberQueue(PHONE_NUMBER_ID);
        queue.offer(batch(1L, 2));

        assertThat(queue.drain(10)).hasSize(2);
        assertThat(queue.drain(10)).isEmpty();
        assertThat(queue.isEmpty()).isTrue();
    }

    private static boolean isCampaign(PendingSend send, long campaignId) {
        return send.batch().campaignId().equals(campaignId);
    }

    private static InFlightBatch batch(long campaignId, int size) {
        return new InFlightBatch(
                new DispatchBatch(campaignId, PHONE_NUMBER_ID, 10L, "token", recipients(size)),
                () -> { });
    }

    private static List<Recipient> recipients(int size) {
        return IntStream.range(0, size)
                .mapToObj(index -> new Recipient((long) index, (long) index, (long) index, "{}"))
                .toList();
    }
}
