MAX_WINRATE = 0.65

def open_lim(lim_trades, closed_trades, asset_conf, worker_cmd_queues, worker_resp_queues):
    #use closed_trades to determine win rate of each broker.
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