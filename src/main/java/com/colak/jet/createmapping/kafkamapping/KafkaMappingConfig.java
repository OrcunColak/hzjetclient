package com.colak.jet.createmapping.kafkamapping;

import lombok.experimental.UtilityClass;

@UtilityClass
public class KafkaMappingConfig {

    public final String kafkaMappingName = "cloud_api_tests_streaming";

    public final String kafkaTopicName = "cloud_api_tests_streaming";

    final String bootstrapServers = "pkc-4r087.us-west2.gcp.confluent.cloud:9092";
    final String confluentUsername = "OUF6NCOFM73HTMEK";
    final String confluentPassword = "Ys72dU5WVdHiITx+tpQE4E2qk4/jFI9/QkGui9Pj0nw6N8YmPreH1HHkNWEpb8QH";
}
