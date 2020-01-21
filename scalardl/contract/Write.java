package com.scalar.jepsen.scalardl;

import com.scalar.ledger.contract.Contract;
import com.scalar.ledger.database.Ledger;
import com.scalar.ledger.exception.ContractContextException;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Write extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!(argument.containsKey("key") && argument.containsKey("value"))) {
      throw new ContractContextException("required key 'key' or 'value' is missing");
    }

    String key = String.valueOf(argument.getInt("key"));
    int value = argument.getInt("value");

    ledger.get(key);
    ledger.put(key, Json.createObjectBuilder().add("value", value).build());

    return null;
  }
}
