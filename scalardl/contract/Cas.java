package com.scalar.jepsen.scalardl;

import com.scalar.dl.ledger.asset.Asset;
import com.scalar.dl.ledger.contract.Contract;
import com.scalar.dl.ledger.database.Ledger;
import com.scalar.dl.ledger.exception.ContractContextException;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Cas extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!(argument.containsKey("key")
        && argument.containsKey("value")
        && argument.containsKey("new_value"))) {
      throw new ContractContextException("required key 'key', 'value' 'new_value' is missing");
    }

    String key = String.valueOf(argument.getInt("key"));
    Optional<Asset> optAsset = ledger.get(key);

    if (!optAsset.isPresent()) {
      throw new ContractContextException("The asset doesn't exist");
    }

    int current = optAsset.get().data().getInt("value");
    int value = argument.getInt("value");
    if (current != value) {
      throw new ContractContextException(
          "The current value isn't the same as the value in the argument");
    }

    int next = argument.getInt("new_value");
    ledger.put(key, Json.createObjectBuilder().add("value", next).build());

    return null;
  }
}
