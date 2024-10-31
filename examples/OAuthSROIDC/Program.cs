﻿// Copyright 2022 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json;

/// <summary>
///     An example demonstrating how to produce a message to 
///     a topic, and then reading it back again using a consumer.
///     The authentication uses the OpenID Connect method of the OAUTHBEARER SASL mechanism.
/// </summary>
namespace Confluent.Kafka.Examples.OAuthOIDC
{
    public class Program
    {
        private const string bootstrapServers = "pkc-vr76rn.us-west4.gcp.confluent.cloud:9092";
        private const String OAuthBearerClientId = "0oa3tq39ol3OLrnQj4x7";
        private const String OAuthBearerClientSecret = "cLXpPxMZ0eA_tc87AL2ZrtZWZzQtPokpYZcOl8gr";
        private const String OAuthBearerTokenEndpointURL = "https://dev-531534.okta.com/oauth2/default/v1/token";
        private const String OAuthBearerScope = "kirk-scope";

        private const string stUrl = "https://psrc-rrk3g1.eastus.azure.confluent.cloud";
        //'basic.auth.user.info': 'OJVS7YASPQKL4ORN:bYMxXMV6E67aTdhMt5R66nWk7u9aB5CP+QCtEnrNgYebiNExfCajTSzX1MqmW2RT'
        //'bearer.auth.credentials.source': 'OAUTHBEARER',
        //'bearer.auth.issuer.endpoint.url': 'https://dev-531534.okta.com/oauth2/default/v1/token',
        //'bearer.auth.client.id': '0oa3tq39ol3OLrnQj4x7',
        //'bearer.auth.client.secret': 'cLXpPxMZ0eA_tc87AL2ZrtZWZzQtPokpYZcOl8gr',
        //'bearer.auth.scope': 'kirk-scope',
        //'bearer.auth.logical.cluster': 'lsrc-8yxg20',
        //'bearer.auth.identity.pool.id': 'pool-wQOq'

        public static async Task Main(string[] args)
        {
            var topicName = "test";
            var groupId = Guid.NewGuid().ToString();

            var commonConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerClientId = OAuthBearerClientId,
                SaslOauthbearerClientSecret = OAuthBearerClientSecret,
                SaslOauthbearerTokenEndpointUrl = OAuthBearerTokenEndpointURL,
                SaslOauthbearerScope = OAuthBearerScope,
            };
            commonConfig.Set("sasl.oauthbearer.extensions", "logicalCluster=lkc-gjr253,identityPoolId=pool-wQOq");

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerClientId = OAuthBearerClientId,
                SaslOauthbearerClientSecret = OAuthBearerClientSecret,
                SaslOauthbearerTokenEndpointUrl = OAuthBearerTokenEndpointURL,
                SaslOauthbearerScope = OAuthBearerScope,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false
            };
            consumerConfig.Set("sasl.oauthbearer.extensions", "logicalCluster=lkc-gjr253,identityPoolId=pool-wQOq");

            var schemaRegistryConfig = new SchemaRegistryConfig();
            schemaRegistryConfig.Set("schema.registry.url", "https://psrc-rrk3g1.eastus.azure.confluent.cloud");
            //schemaRegistryConfig.Set("schema.registry.basic.auth.user.info", "OJVS7YASPQKL4ORN:bYMxXMV6E67aTdhMt5R66nWk7u9aB5CP+QCtEnrNgYebiNExfCajTSzX1MqmW2RT");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.credentials.source", "OAUTHBEARER");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.issuer.endpoint.url", "https://dev-531534.okta.com/oauth2/default/v1/token");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.client.id", "0oa3tq39ol3OLrnQj4x7");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.client.secret", "cLXpPxMZ0eA_tc87AL2ZrtZWZzQtPokpYZcOl8gr");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.scope", "kirk-scope");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.logical.cluster", "lsrc-8yxg20");
            schemaRegistryConfig.Set("schema.registry.bearer.auth.identity.pool.id", "pool-wQOq");

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new ProducerBuilder<String, GenericRecord>(commonConfig)
                .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
                .Build())
            using (var consumer = new ConsumerBuilder<String, GenericRecord>(consumerConfig)
                        .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync<GenericRecord>())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                 .Build())
            {
                consumer.Subscribe(topicName);

                var cancelled = false;
                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                    cts.Cancel();
                };

                try
                {
                    var messageSchema = (Avro.RecordSchema)Avro.Schema.Parse(File.ReadAllText("test.avsc"));

                    var msg = new GenericRecord(messageSchema);
                    msg.Add("name", "test from DotNet SR OIDC SUCCESS");
                    msg.Add("age", 21);

                    try
                    {
                        var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, GenericRecord> { Value = msg });
                        Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}, {msg}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }

                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                        try
                        {
                            consumer.StoreOffset(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }

                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        private static void createTopic(ClientConfig config, String topicName)
        {
            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                adminClient.CreateTopicsAsync(new TopicSpecification[] {
                            new TopicSpecification { Name = topicName, ReplicationFactor = 3, NumPartitions = 1 } }).Wait(); ;
            }
        }
    }

}
