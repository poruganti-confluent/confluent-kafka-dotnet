using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Rest.Authentication
{
    internal class BearerAccessTokenHandler
    {
        public Token GetAccessToken(BearerConfig config)
        {
            using (HttpClientHandler httpClientHandler = new HttpClientHandler())
            {
                httpClientHandler.ServerCertificateCustomValidationCallback = CertificateValidationCallBack;
                using (HttpClient client = new HttpClient(httpClientHandler))
                {
                    return GetAccessToken(client, config);
                }
            }
        }

        private Token GetAccessToken(HttpClient client, BearerConfig config)
        {
            string tokenUrl = config.tokenUrl;

            string grant_type = "client_credentials";
            string client_id = config.clientId;
            string client_secret = config.clientSecret;
            string scope = config.scope;

            var httpContent = new Dictionary<string, string>
                {
                    {"grant_type", grant_type},
                    {"client_id", client_id},
                    {"client_secret", client_secret},
                    {"scope", scope }
                };

            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.Add("cache-control", "no-cache");

            HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, tokenUrl);
            req.Content = new FormUrlEncodedContent(httpContent);
            req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/x-www-form-urlencoded");

            try
            {
                var tokenResponse = Task.Run<HttpResponseMessage>(async () => await client.SendAsync(req));
                var jsonContent = Task.Run<string>(async () => await tokenResponse.Result.Content.ReadAsStringAsync());
                Token accessToken = JsonConvert.DeserializeObject<Token>(jsonContent.Result);
                return accessToken;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        private bool CertificateValidationCallBack(
        object sender,
        System.Security.Cryptography.X509Certificates.X509Certificate certificate,
        System.Security.Cryptography.X509Certificates.X509Chain chain,
        System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            // If the certificate is a valid, signed certificate, return true.
            if (sslPolicyErrors == System.Net.Security.SslPolicyErrors.None)
            {
                return true;
            }

            // If there are errors in the certificate chain, look at each error to determine the cause.
            if ((sslPolicyErrors & System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors) != 0)
            {
                if (chain != null && chain.ChainStatus != null)
                {
                    foreach (System.Security.Cryptography.X509Certificates.X509ChainStatus status in chain.ChainStatus)
                    {
                        if ((certificate.Subject == certificate.Issuer) &&
                           (status.Status == System.Security.Cryptography.X509Certificates.X509ChainStatusFlags.UntrustedRoot))
                        {
                            // Self-signed certificates with an untrusted root are valid. 
                            continue;
                        }
                        else
                        {
                            if (status.Status != System.Security.Cryptography.X509Certificates.X509ChainStatusFlags.NoError)
                            {
                                // If there are any other errors in the certificate chain, the certificate is invalid,
                                // so the method returns false.
                                return false;
                            }
                        }
                    }
                }

                // When processing reaches this line, the only errors in the certificate chain are 
                // untrusted root errors for self-signed certificates. These certificates are valid
                // for default Exchange server installations, so return true.
                return true;
            }


            /* overcome localhost and 127.0.0.1 issue */
            if ((sslPolicyErrors & System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {
                if (certificate.Subject.Contains("localhost"))
                {
                    HttpRequestMessage castSender = sender as HttpRequestMessage;
                    if (null != castSender)
                    {
                        if (castSender.RequestUri.Host.Contains("127.0.0.1"))
                        {
                            return true;
                        }
                    }
                }
            }

            return false;

        }

        public class BearerConfig
        {
            public string tokenUrl { get; set; }
            public string clientId { get; set; }
            public string clientSecret { get; set; }
            public string scope { get; set; }
        }

        public class Token
        {
            [JsonProperty("access_token")]
            public string AccessToken { get; set; }

            [JsonProperty("token_type")]
            public string TokenType { get; set; }

            [JsonProperty("expires_in")]
            public int ExpiresIn { get; set; }

            [JsonProperty("refresh_token")]
            public string RefreshToken { get; set; }
        }

    }
}
