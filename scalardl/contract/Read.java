package com.scalar.jepsen.scalardl;

import com.scalar.ledger.asset.Asset;
import com.scalar.ledger.contract.Contract;
import com.scalar.ledger.database.Ledger;
import com.scalar.ledger.exception.ContractContextException;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Read extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!argument.containsKey("key")) {
      throw new ContractContextException("required key 'key' is missing");
    }

    String key = String.valueOf(argument.getInt("key"));
    Optional<Asset> optAsset = ledger.get(key);

    if (!optAsset.isPresent()) {
      throw new ContractContextException("The asset doesn't exist");
    }

    return Json.createObjectBuilder().add("value", optAsset.get().data().getInt("value")).build();
  }
}
