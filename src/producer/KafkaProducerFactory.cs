using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;

namespace producer
{
    public class KafkaProducerFactory
    {
        private readonly KafkaConfiguration _configuration;

        public KafkaProducerFactory()
        {
            _configuration = new KafkaConfiguration();
        }

        public IProducer<string, string> Create()
        {
            var config = new ProducerConfig(_configuration.GetConfiguration());
            var builder = new ProducerBuilder<string, string>(config);
            builder.SetErrorHandler(OnKafkaError);
            return builder.Build();
        }
        
        private void OnKafkaError(IProducer<string, string> producer, Error error)
        {
            if (error.IsFatal)
                Environment.FailFast($"Fatal error in Kafka producer: {error.Reason}. Shutting down...");
        }

        public class KafkaConfiguration
        {
            private const string KEY_PREFIX = "CONFLUENT_KAFKA_WORKSHOP_";

            private string Key(string keyName) => string.Join("", KEY_PREFIX, keyName.ToUpper().Replace('.', '_'));

            private Tuple<string, string> GetConfiguration(string key)
            {
                var value = Environment.GetEnvironmentVariable(key, EnvironmentVariableTarget.Process);

                if (string.IsNullOrWhiteSpace(value))
                {
                    return null;
                }

                return Tuple.Create<string, string>(key, value);
            }

            public ProducerConfig GetConfiguration()
            {
                var configurationKeys = new[]
                {
                    "bootstrap.servers",
                    "broker.version.fallback",
                    "api.version.fallback.ms",
                    "ssl.ca.location",
                    "sasl.username",
                    "sasl.password",
                    "sasl.mechanisms",
                    "security.protocol",
                };

                var config = configurationKeys
                             .Select(key => GetConfiguration(key))
                             .Where(pair => pair != null)
                             .Select(pair => new KeyValuePair<string, string>(pair.Item1, pair.Item2))
                             .ToList();
                var producerConfig = new ProducerConfig(config)
                {
                    ApiVersionRequest = true,
                    BrokerVersionFallback = "0.10.0.0",
                    ApiVersionFallbackMs = 0,
                    SaslMechanism = SaslMechanism.Plain,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    RequestTimeoutMs = 3000
                };
                return producerConfig;
            }
        }
    }
}
