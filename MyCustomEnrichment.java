/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.acme;

import java.util.ArrayList;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent;
import com.snowplowanalytics.snowplow.enrich.common.enrichments.IUserEnrichment;

public class MyCustomEnrichment implements IUserEnrichment {

	public void configure(JsonNode config) throws Exception {}

	public ArrayList<JsonNode> createDerivedContexts(EnrichedEvent event, JsonNode unstructEvent, ArrayList<JsonNode> existingContexts) {
		ArrayList<JsonNode> output = new ArrayList<>();
		if (event.getV_tracker() != null) {
			JsonNodeFactory factory = JsonNodeFactory.instance;
			ObjectNode root = factory.objectNode();
			root.put("schema", "iglu:com.acme/my_schema/jsonschema/1-0-0");
			ObjectNode data = factory.objectNode();
			data.put("trackerType", event.getV_tracker().split("-")[0]);
			root.put("data", data);
			output.add(root);
		}
		if (unstructEvent != null && (unstructEvent.get("schema").asText() == "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1")) {
			String linkType = unstructEvent.get("data").get("target").asText().contains("product") ? "product" : "unknown";
			JsonNodeFactory factory = JsonNodeFactory.instance;
			ObjectNode root = factory.objectNode();
			root.put("schema", "iglu:com.acme/fixed_schema/jsonschema/1-0-0");
			ObjectNode data = factory.objectNode();
			data.put("linkType", linkType);
			root.put("data", data);
			output.add(root);
		}
		return output;
	}
}
