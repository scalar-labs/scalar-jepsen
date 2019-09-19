package com.scalar.jepsen.scalardl;

import com.scalar.ledger.asset.Asset;
import com.scalar.ledger.contract.Contract;
import com.scalar.ledger.exception.ContractContextException;
import com.scalar.ledger.ledger.Ledger;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Create extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!(argument.containsKey("id") && argument.containsKey("balance"))) {
      throw new ContractContextException("required key 'id' or 'balance' is missing");
    }

    String id = String.valueOf(argument.getInt("id"));
    Optional<Asset> optAsset = ledger.get(id);

    if (optAsset.isPresent()) {
      throw new ContractContextException("The asset already exists");
    }

    int balance = argument.getInt("balance");
    ledger.put(id, Json.createObjectBuilder().add("balance", balance).build());

    return null;
  }
}
