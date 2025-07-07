from trade import Trade

MAX_WINRATE = 0.65
BROKERS = ["icmarkets", "exness", "fxtm", "eightcap", "xm"]

def open_lim(lim_open, lim_closed, closed_trades, asset_conf, worker_cmd_queues, worker_resp_queues):
    #for broker in BROKERS:
        #if has_lim(broker, lim_open):
            #continue
        #winrate = get_winrate(broker, lim_closed, closed_trades)
        #if winrate > MAX_WINRATE:
            #lim_trade = init_lim_trade(broker, asset_conf, balances_df)
            #lim_trade = place_lim_trade(lim_trade, broker, worker_cmd_queues, worker_resp_queues)
            #if lim_trade.status == "open":
                #lim_open.append(lim_trade)



    #use closed_trades and lim_closed to determine win rate of each broker.
        #if number of closed trades at that broker < 5, skip
    
    #for each broker:
        #if there is already a lim trade open at this broker, skip
        #if win rate >= MAX_WINRATE:
            #lim_trade = init_lim_trade() -> function that does param calculations for trade, returns trade object
            #place lim trade on broker queue
            #if successful : 
                #lim_trades.append(lim_trade)
    
    #return lim_trades
    pass