// Copyright 2016-2018 Confluent Inc.
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

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Text;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A HTTP authentication header value provider implementing the 'Basic' scheme.
    ///
    ///     See: https://datatracker.ietf.org/doc/html/rfc7617
    /// </summary>
    public class BearerAuthenticationHeaderValueProvider : IAuthenticationHeaderValueProvider
    {
        readonly AuthenticationHeaderValue authenticationHeader;
        readonly List<AuthenticationHeaderValue> extensionHeaders = new List<AuthenticationHeaderValue>();

        /// <summary>
        ///     Initialize a new instance of the BearerAuthenticationHeaderValueProvider class.
        /// </summary>
        /// <param name="token">
        ///     The bearer token
        /// </param>
        /// <param name="logicalCluster">
        ///     The logical cluster
        /// </param>
        /// <param name="identityPoolId">
        ///     The Identity pool id
        /// </param>

        public BearerAuthenticationHeaderValueProvider(string token, string logicalCluster, string identityPoolId)
        {
            authenticationHeader = new AuthenticationHeaderValue("Bearer", token);

            extensionHeaders.Add(new AuthenticationHeaderValue("target-sr-cluster", logicalCluster));
            extensionHeaders.Add(new AuthenticationHeaderValue("Confluent-Identity-Pool-Id", identityPoolId));
        }

        /// <inheritdoc/>
        public AuthenticationHeaderValue GetAuthenticationHeader() => authenticationHeader;

        /// <inheritdoc/>
        public List<AuthenticationHeaderValue> GetExtensionHeaders() => extensionHeaders;
    }
}
