from mmm.nasdaq import load_actions, create_trajectory_summaries


if __name__ == "__main__":
    actions = load_actions("~/data/nasdaq-itch/prep/S101521-v50/AAPL.bin.zst")
    summaries = create_trajectory_summaries(actions)
    print(summaries[0].reference)
    print(summaries[0].trajectory)
    print(summaries[0].timestamps)
    print(summaries[0].is_buy)
    print(summaries[0].price)
    print(summaries[0].shares)
    print(summaries[0].executed_shares)
    print(summaries[0].executed_with_price_shares)
    print(summaries[0].cancelled_shares)

