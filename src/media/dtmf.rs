use std::sync::atomic::{AtomicU8, AtomicU16};

use audio_codec::telephone_event::{event_to_digit, parse_telephone_event};

/// Maximum RFC 4733 named-event code (DTMF "A"-"D" end at 15).
const MAX_DTMF_EVENT: u8 = 15;

pub struct DtmfDetector {
    // Track the last seen event to avoid repeated events
    last_event: AtomicU8,
    last_duration: AtomicU16,
    pub refer: Option<bool>,
    pub suppress_dtmf_forward: bool,
}

impl DtmfDetector {
    pub fn new() -> Self {
        Self {
            last_event: AtomicU8::new(0xFF),
            last_duration: AtomicU16::new(0),
            refer: None,
            suppress_dtmf_forward: false,
        }
    }

    // Detect DTMF events from RTP payload as specified in RFC 4733
    pub fn detect_rtp(&self, payload_type: u8, payload: &[u8]) -> Option<String> {
        // RFC 4733 defines DTMF events with payload types 96-127 (dynamic)
        // However, we'll be more lenient and just check if the payload has the right format
        if payload.len() < 4 {
            return None;
        }

        // Generally, telephone-event payload type is in dynamic range 96-127
        if !(96..=127).contains(&payload_type) {
            return None;
        }

        // Parse the RFC 4733 payload (last event block wins per the RFC).
        let parsed = parse_telephone_event(payload)?;
        if parsed.event > MAX_DTMF_EVENT {
            return None;
        }

        // Get current duration
        let current_event = parsed.event;
        let current_duration = parsed.duration;
        let last_event = self
            .last_event
            .swap(current_event, std::sync::atomic::Ordering::Relaxed);
        let last_duration = self
            .last_duration
            .swap(current_duration, std::sync::atomic::Ordering::Relaxed);

        if current_event == last_event && current_duration >= last_duration {
            return None;
        }

        Some(event_to_digit(current_event).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 4733 event codes used by the tests.
    const DTMF_EVENT_0: u8 = 0;
    const DTMF_EVENT_5: u8 = 5;
    const DTMF_EVENT_6: u8 = 6;

    #[test]
    fn test_dtmf_payload_parse() {
        let detector = DtmfDetector::new();

        // Valid DTMF payload for digit "1" with end bit set
        // Event: 1, End bit: 1, Reserved: 0, Volume: 10, Duration: 160
        let payload = [1, 0x8A, 0, 160]; // 0x8A = 10001010 (end=1, reserved=0, volume=10)
        assert_eq!(detector.detect_rtp(101, &payload), Some("1".to_string()));

        // The specific case [2, 138, 3, 32]:
        // 138 decimal = 10001010 binary → end bit set, volume 10;
        // duration is 3 * 256 + 32 = 800.
        let detector = DtmfDetector::new();
        let payload = [2, 138, 3, 32];
        assert_eq!(detector.detect_rtp(101, &payload), Some("2".to_string()));

        // Invalid event code (> 15)
        let detector = DtmfDetector::new();
        let payload = [20, 0x80, 10, 100];
        assert_eq!(detector.detect_rtp(101, &payload), None);

        // Too short payload
        let detector = DtmfDetector::new();
        let payload = [1, 0x80, 10]; // Missing duration byte
        assert_eq!(detector.detect_rtp(101, &payload), None);
    }

    #[test]
    fn test_dtmf_detection() {
        let detector = DtmfDetector::new();

        // Test basic detection
        {
            // Valid DTMF payload for digit "5" with end bit set
            let payload = [DTMF_EVENT_5, 0x80, 10, 100];

            // Use payload_type 101 (typical for telephone-event)
            let digit = detector.detect_rtp(101, &payload);
            assert_eq!(digit, Some("5".to_string()));

            // Should reject payloads with invalid payload type
            let digit = detector.detect_rtp(0, &payload);
            assert_eq!(digit, None);

            // Should reject payloads with end bit not set
            let payload = [DTMF_EVENT_5, 0x00, 10, 100];
            let digit = detector.detect_rtp(101, &payload);
            assert_eq!(digit, None);
        }

        // Test duplicate detection
        {
            let detector = DtmfDetector::new(); // Use a fresh detector

            // First event
            let payload1 = [DTMF_EVENT_5, 0x80, 0, 100]; // Duration 100
            let digit1 = detector.detect_rtp(101, &payload1);
            assert_eq!(digit1, Some("5".to_string()));

            // Similar duration - should be rejected as duplicate
            let payload2 = [DTMF_EVENT_5, 0x80, 0, 100]; // Duration 150 (similar)
            let digit2 = detector.detect_rtp(101, &payload2);
            assert_eq!(digit2, None);

            // Different event - should be detected
            let payload4 = [DTMF_EVENT_6, 0x80, 1, 8]; // Event 6 ("6" key)
            let digit4 = detector.detect_rtp(101, &payload4);
            assert_eq!(digit4, Some("6".to_string()));
        }
    }

    #[test]
    fn test_dtmf_digit_0_first_press() {
        let detector = DtmfDetector::new();

        let payload = [DTMF_EVENT_0, 0x80, 0, 160]; // Event 0, end bit set
        let digit = detector.detect_rtp(101, &payload);
        assert_eq!(
            digit,
            Some("0".to_string()),
            "First press of digit '0' should be recognized"
        );

        // Duplicate should be ignored
        let payload2 = [DTMF_EVENT_0, 0x80, 0, 160]; // Same event, same duration
        let digit2 = detector.detect_rtp(101, &payload2);
        assert_eq!(digit2, None, "Duplicate DTMF should be filtered out");

        // New press with smaller duration (new key press started) should be detected
        let payload3 = [DTMF_EVENT_0, 0x80, 0, 80]; // Smaller duration = new press
        let digit3 = detector.detect_rtp(101, &payload3);
        assert_eq!(
            digit3,
            Some("0".to_string()),
            "Second press with smaller duration should be recognized as new press"
        );
    }
}
