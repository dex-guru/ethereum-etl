def get_balances(
    web3,
    addresses,
    block_number,
    batch_size=1000,
    batch_web3=None,
    # batch_size_limit=10000
):
    if batch_web3 is None:
        batch_web3 = web3
    balances = {}
    for i in range(0, len(addresses), batch_size):
        batch_addresses = addresses[i:i + batch_size]
        batch_balances = batch_web3.eth.getBalance(batch_addresses, block_number)
        balances.update(dict(zip(batch_addresses, batch_balances)))
    return balances
