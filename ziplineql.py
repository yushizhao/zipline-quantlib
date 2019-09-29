import pandas as pd
import numpy as np
from scipy.stats import norm
from zipline.api import get_asset_info
import QuantLib as ql

def delta_BS (asset, data, volatility = 0.20, iRate = 0.001, dRate = 0):
	'''Calculate delta use Black-Scholes-Merto. 
	May be modified to support mutiple symbol and other greeks,
	something like BS(["delta","vega"],assets).

	Parameters
	----------
	asset is something like symbol("AAPL").
	data is the corresponding data[asset], we don't want to pass the whole data.
	others are QuantLib parameters.    
	'''    
    # zipline input
	strike = get_asset_info(asset,"strike")
	cp = get_asset_info(asset,"callput")
	dtTrade = pd.to_datetime(data.last_traded,utc=True)
	dtExpiry = pd.to_datetime(get_asset_info(asset,"expiration"),utc=True)
	spot = data.close
	ask = data.ask
	bid = data.bid
	daysToExpiry = (dtExpiry-dtTrade).days      
	daysToExpiry = daysToExpiry/365.0
	
	day_count = ql.Actual365Fixed()
	calendar = ql.UnitedStates()
	calculation_date = ql.Date(dtTrade.day, dtTrade.month, dtTrade.year)
	maturity_date = ql.Date(dtExpiry.day,dtExpiry.month,dtExpiry.year)
	ql.Settings.instance().evaluationDate = calculation_date
	# construct the European Option
	if cp =='C':
		payoff = ql.PlainVanillaPayoff(1, strike)
	else:
		payoff = ql.PlainVanillaPayoff(-1, strike)
	exercise = ql.EuropeanExercise(maturity_date)
	european_option = ql.VanillaOption(payoff, exercise)

	#The Black-Scholes-Merto process is constructed here.
	spot_handle = ql.QuoteHandle(
		ql.SimpleQuote(spot)
	)
	flat_ts = ql.YieldTermStructureHandle(
		ql.FlatForward(calculation_date, iRate, day_count)
	)
	dividend_yield = ql.YieldTermStructureHandle(
		ql.FlatForward(calculation_date, dRate, day_count)
	)
	flat_vol_ts = ql.BlackVolTermStructureHandle(
		ql.BlackConstantVol(calculation_date, calendar, volatility, day_count)
	)
	bsm_process = ql.BlackScholesMertonProcess(spot_handle, 
										dividend_yield, 
										flat_ts, 
										flat_vol_ts)

	imvol = european_option.impliedVolatility(ask, bsm_process, accuracy=1.0e-4, \
									  maxEvaluations=1000, minVol=1.0e-4, maxVol=4.0)
	Fwd = spot * np.exp((iRate-dRate)*daysToExpiry)
	delta = norm().cdf(np.log(Fwd/strike)/(imvol*np.sqrt(daysToExpiry)))
	return delta