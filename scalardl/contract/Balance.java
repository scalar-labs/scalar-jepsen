package com.scalar.jepsen.scalardl;

import com.scalar.dl.ledger.asset.Asset;
import com.scalar.dl.ledger.contract.Contract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.database.Ledger;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Balance extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!argument.containsKey("id")) {
      throw new ContractContextException("required key 'id' is missing");
    }

    String id = String.valueOf(argument.getInt("id"));
    Optional<Asset> optAsset = ledger.get(id);

    if (!optAsset.isPresent()) {
      throw new ContractContextException("The asset doesn't exist");
    }

    return Json.createObjectBuilder()
        .add("balance", optAsset.get().data().getInt("balance"))
        .add("age", optAsset.get().age())
        .build();
  }
}
