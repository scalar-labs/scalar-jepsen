package com.scalar.jepsen.scalardl;

import com.scalar.ledger.asset.Asset;
import com.scalar.ledger.contract.Contract;
import com.scalar.ledger.exception.ContractContextException;
import com.scalar.ledger.ledger.Ledger;
import java.util.Optional;
import javax.json.Json;
import javax.json.JsonObject;

public class Transfer extends Contract {
  @Override
  public JsonObject invoke(Ledger ledger, JsonObject argument, Optional<JsonObject> property) {
    if (!(argument.containsKey("from")
        && argument.containsKey("to")
        && argument.containsKey("amount"))) {
      throw new ContractContextException("required keys 'from', 'to' and 'amount' is missing");
    }

    String from = String.valueOf(argument.getInt("from"));
    String to = String.valueOf(argument.getInt("to"));
    Optional<Asset> fromAsset = ledger.get(from);
    Optional<Asset> toAsset = ledger.get(to);

    if (!fromAsset.isPresent() || !toAsset.isPresent()) {
      throw new ContractContextException("The asset doesn't exist");
    }

    int curFrom = fromAsset.get().data().getInt("balance");
    int curTo = toAsset.get().data().getInt("balance");

    int amount = argument.getInt("amount");
    int newFrom = curFrom - amount;
    int newTo = curTo + amount;

    ledger.put(from, Json.createObjectBuilder().add("balance", newFrom).build());
    ledger.put(to, Json.createObjectBuilder().add("balance", newTo).build());

    return null;
  }
}
