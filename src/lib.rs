use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, HashSet};

// --- Internal Rust types (not exposed to Python) for the hot path ---

#[derive(Clone)]
struct MarketEventInput {
    symbol: String,
    close: f64,
    timestamp_secs: i64,
    date_days: i64, // days since Unix epoch for date comparison
    asset_class: String,
}

struct FillInput {
    symbol: String,
    quantity: f64,
    fill_price: f64,
    commission: f64,
    multiplier: f64,
    timestamp_secs: i64,
}

struct PositionMetaInput {
    asset_class: String,
    underlying: Option<String>,
    strike: Option<f64>,
    expiry_days: Option<i64>, // days since Unix epoch
    option_type: Option<String>,
}

// --- Position (PyO3-exposed, matches Python Position interface) ---

#[pyclass]
#[derive(Clone)]
pub struct Position {
    #[pyo3(get, set)]
    pub symbol: String,
    #[pyo3(get, set)]
    pub quantity: f64,
    #[pyo3(get, set)]
    pub avg_cost: f64,
    #[pyo3(get, set)]
    pub multiplier: f64,
    #[pyo3(get, set)]
    pub asset_class: String,
    #[pyo3(get, set)]
    pub underlying: Option<String>,
    #[pyo3(get, set)]
    pub strike: Option<f64>,
    #[pyo3(get, set)]
    pub expiry: Option<i64>, // stored as days since epoch; Python can convert back to datetime
    #[pyo3(get, set)]
    pub option_type: Option<String>,
}

#[pymethods]
impl Position {
    fn market_value(&self, current_price: f64) -> f64 {
        self.quantity * current_price * self.multiplier
    }
}

// --- RustPortfolio ---

#[pyclass]
#[derive(Clone)]
pub struct RustPortfolio {
    #[pyo3(get)]
    pub cash: f64,
    initial_capital: f64,
    pub positions: HashMap<String, Position>,
    pub last_prices: HashMap<String, f64>,
    position_value: f64,
    last_expiry_check: Option<i64>, // date_days
    pub trade_timestamps: Vec<i64>,
    pub trade_symbols: Vec<String>,
    pub trade_qtys: Vec<f64>,
    pub trade_prices: Vec<f64>,
    pub trade_comms: Vec<f64>,
}

#[pymethods]
impl RustPortfolio {
    #[new]
    pub fn new(initial_capital: f64) -> Self {
        RustPortfolio {
            cash: initial_capital,
            initial_capital,
            positions: HashMap::new(),
            last_prices: HashMap::new(),
            position_value: 0.0,
            last_expiry_check: None,
            trade_timestamps: Vec::new(),
            trade_symbols: Vec::new(),
            trade_qtys: Vec::new(),
            trade_prices: Vec::new(),
            trade_comms: Vec::new(),
        }
    }

    /// Update prices from a market event. Called from Python with extracted event fields.
    /// timestamp_secs: Unix timestamp of event; date_days: days since Unix epoch for event date.
    pub fn update_prices(
        &mut self,
        symbol: String,
        close: f64,
        timestamp_secs: i64,
        date_days: i64,
        asset_class: String,
    ) {
        let event = MarketEventInput {
            symbol: symbol.clone(),
            close,
            timestamp_secs,
            date_days,
            asset_class,
        };
        self.update_prices_inner(event);
    }

    /// Apply a fill. position_meta can be None or a dict with asset_class, underlying, strike, expiry_days, option_type.
    pub fn apply_fill(
        &mut self,
        symbol: String,
        quantity: f64,
        fill_price: f64,
        commission: f64,
        multiplier: f64,
        timestamp_secs: i64,
        position_meta: Option<&Bound<'_, PyAny>>,
    ) {
        let meta = parse_position_meta(position_meta);
        let fill = FillInput {
            symbol: symbol.clone(),
            quantity,
            fill_price,
            commission,
            multiplier,
            timestamp_secs,
        };
        self.apply_fill_inner(fill, meta);
    }

    /// Called from Python with the full event object (for compatibility). Extracts fields and calls update_prices.
    pub fn update_prices_from_event(&mut self, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let symbol: String = event.getattr("symbol")?.extract()?;
        let close: f64 = event.getattr("close")?.extract()?;
        let timestamp = event.getattr("timestamp")?;
        let timestamp_secs = py_datetime_to_secs(&timestamp)?;
        let date_days = py_datetime_to_date_days(&timestamp)?;
        let asset_class = asset_class_to_string(&event.getattr("asset_class")?)?;
        self.update_prices(symbol, close, timestamp_secs, date_days, asset_class);
        Ok(())
    }

    /// Called from Python with fill and optional meta dict.
    pub fn apply_fill_from_python(
        &mut self,
        fill: &Bound<'_, PyAny>,
        position_meta: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let symbol: String = fill.getattr("symbol")?.extract()?;
        let quantity: f64 = fill.getattr("quantity")?.extract()?;
        let fill_price: f64 = fill.getattr("fill_price")?.extract()?;
        let commission: f64 = fill.getattr("commission")?.extract()?;
        let multiplier: f64 = fill.getattr("multiplier")?.extract()?;
        let timestamp = fill.getattr("timestamp")?;
        let timestamp_secs = py_datetime_to_secs(&timestamp)?;
        self.apply_fill(symbol, quantity, fill_price, commission, multiplier, timestamp_secs, position_meta);
        Ok(())
    }

    #[getter]
    pub fn equity(&self) -> f64 {
        let mv: f64 = self
            .positions
            .iter()
            .map(|(sym, pos)| {
                let price = self.last_prices.get(sym).copied().unwrap_or(pos.avg_cost);
                pos.quantity * price * pos.multiplier
            })
            .sum();
        self.cash + mv
    }

    #[getter]
    pub fn initial_capital(&self) -> f64 {
        self.initial_capital
    }

    /// Return a dict of symbol -> last price (read-only view).
    pub fn prices(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.last_prices {
            dict.set_item(k.as_str(), *v)?;
        }
        Ok(dict.into())
    }

    /// Return trade journal as a dict of lists: timestamp, symbol, quantity, fill_price, commission.
    pub fn trade_journal(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("timestamp", self.trade_timestamps.clone())?;
        dict.set_item("symbol", self.trade_symbols.clone())?;
        dict.set_item("quantity", self.trade_qtys.clone())?;
        dict.set_item("fill_price", self.trade_prices.clone())?;
        dict.set_item("commission", self.trade_comms.clone())?;
        Ok(dict.into())
    }

    /// Underlying view: dict[underlying_sym] -> { "price", "option_symbols", "equity_position" }.
    /// equity_position is a Position or None; option_symbols is list of str.
    pub fn underlying_view(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let view = PyDict::new(py);
        for (sym, pos) in &self.positions {
            let asset_class = pos.asset_class.as_str();
            let price = self.last_prices.get(sym).copied().unwrap_or(pos.avg_cost);
            if asset_class == "EQUITY" {
                let entry = PyDict::new(py);
                entry.set_item("price", price)?;
                entry.set_item("option_symbols", Vec::<String>::new())?;
                entry.set_item("equity_position", pos.clone())?;
                view.set_item(sym.as_str(), entry)?;
            } else {
                let u = pos.underlying.as_deref().unwrap_or("");
                if !view.contains(u)? {
                    let entry = PyDict::new(py);
                    let u_price = self.last_prices.get(u).copied().unwrap_or(0.0);
                    entry.set_item("price", u_price)?;
                    entry.set_item("option_symbols", Vec::<String>::new())?;
                    let eq_pos = self.positions.get(u).filter(|p| p.asset_class.as_str() == "EQUITY").cloned();
                    entry.set_item("equity_position", eq_pos)?;
                    view.set_item(u, entry)?;
                }
                let entry = view.get_item(u)?.ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(u.to_string()))?;
                let opt_list = entry.get_item("option_symbols")?;
                opt_list.call_method1("append", (sym.as_str(),))?;
            }
        }
        for (u_str, pos) in &self.positions {
            if pos.asset_class.as_str() == "EQUITY" {
                if let Some(entry) = view.get_item(u_str.as_str())? {
                    entry.set_item("equity_position", pos.clone())?;
                }
            }
        }
        Ok(view.into())
    }

    /// Return positions as a Python dict symbol -> Position.
    pub fn get_positions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (sym, pos) in &self.positions {
            dict.set_item(sym.as_str(), pos.clone())?;
        }
        Ok(dict.into())
    }

    #[getter]
    pub fn position_count(&self) -> usize {
        self.positions.len()
    }

    /// Mutable cash setter for history and external use.
    pub fn set_cash(&mut self, cash: f64) {
        self.cash = cash;
    }
}

impl RustPortfolio {
    fn update_prices_inner(&mut self, event: MarketEventInput) {
        let sym = event.symbol.clone();
        let new_price = event.close;
        if let Some(pos) = self.positions.get(&sym) {
            let old_price = self.last_prices.get(&sym).copied().unwrap_or(pos.avg_cost);
            let delta = (new_price - old_price) * pos.quantity * pos.multiplier;
            self.position_value += delta;
        }
        self.last_prices.insert(sym.clone(), new_price);

        if event.asset_class == "OPTION" {
            if Some(event.date_days) != self.last_expiry_check {
                self.expire_options(event.date_days);
                self.last_expiry_check = Some(event.date_days);
            }
        }
    }

    fn expire_options(&mut self, current_date_days: i64) {
        let expired: Vec<(String, Position)> = self
            .positions
            .iter()
            .filter(|(_, pos)| {
                pos.asset_class.as_str() == "OPTION"
                    && pos.expiry.map(|e| e <= current_date_days).unwrap_or(false)
            })
            .map(|(sym, pos)| (sym.clone(), pos.clone()))
            .collect();
        for (sym, pos) in expired {
            let market_price = self.last_prices.get(&sym).copied().unwrap_or(pos.avg_cost);
            self.position_value -= pos.quantity * market_price * pos.multiplier;
            self.positions.remove(&sym);

            let underlying_price = pos
                .underlying
                .as_ref()
                .and_then(|u| self.last_prices.get(u).copied())
                .unwrap_or(0.0);
            let strike = pos.strike.unwrap_or(0.0);
            let intrinsic = if pos.option_type.as_deref() == Some("C") {
                (underlying_price - strike).max(0.0)
            } else {
                (strike - underlying_price).max(0.0)
            };
            let payout = intrinsic * pos.quantity * pos.multiplier;
            self.cash += payout;
        }
    }

    fn apply_fill_inner(&mut self, fill: FillInput, meta: PositionMetaInput) {
        let cost = fill.quantity * fill.fill_price * fill.multiplier;
        self.cash -= cost + fill.commission;

        let sym = fill.symbol.clone();
        let market_price = self.last_prices.get(&sym).copied().unwrap_or(fill.fill_price);

        if let Some(pos) = self.positions.get_mut(&sym) {
            self.position_value -= pos.quantity * market_price * pos.multiplier;
            let new_qty = pos.quantity + fill.quantity;
            if new_qty.abs() < 1e-9 {
                self.positions.remove(&sym);
            } else {
                if (pos.quantity > 0.0) == (fill.quantity > 0.0) {
                    pos.avg_cost = ((pos.quantity * pos.avg_cost) + (fill.quantity * fill.fill_price)) / new_qty;
                }
                pos.quantity = new_qty;
                self.position_value += pos.quantity * market_price * pos.multiplier;
            }
        } else {
            let new_pos = Position {
                symbol: sym.clone(),
                quantity: fill.quantity,
                avg_cost: fill.fill_price,
                multiplier: fill.multiplier,
                asset_class: meta.asset_class,
                underlying: meta.underlying,
                strike: meta.strike,
                expiry: meta.expiry_days,
                option_type: meta.option_type,
            };
            self.position_value += new_pos.quantity * market_price * new_pos.multiplier;
            self.positions.insert(sym.clone(), new_pos);
        }

        self.trade_timestamps.push(fill.timestamp_secs);
        self.trade_symbols.push(sym);
        self.trade_qtys.push(fill.quantity);
        self.trade_prices.push(fill.fill_price);
        self.trade_comms.push(fill.commission);
    }
}

fn parse_position_meta(meta: Option<&Bound<'_, PyAny>>) -> PositionMetaInput {
    let Some(meta) = meta else {
        return PositionMetaInput {
            asset_class: "EQUITY".to_string(),
            underlying: None,
            strike: None,
            expiry_days: None,
            option_type: None,
        };
    };
    let Ok(dict) = meta.cast::<PyDict>() else {
        return PositionMetaInput {
            asset_class: "EQUITY".to_string(),
            underlying: None,
            strike: None,
            expiry_days: None,
            option_type: None,
        };
    };
    let asset_class = dict
        .get_item("asset_class")
        .ok()
        .flatten()
        .and_then(|o: Bound<'_, PyAny>| o.extract::<String>().ok())
        .unwrap_or_else(|| "EQUITY".to_string());
    let underlying = dict.get_item("underlying").ok().flatten().and_then(|o: Bound<'_, PyAny>| o.extract::<String>().ok());
    let strike = dict.get_item("strike").ok().flatten().and_then(|o: Bound<'_, PyAny>| o.extract::<f64>().ok());
    let expiry_days = dict.get_item("expiry_days").ok().flatten().and_then(|o: Bound<'_, PyAny>| o.extract::<i64>().ok());
    let option_type = dict.get_item("option_type").ok().flatten().and_then(|o: Bound<'_, PyAny>| o.extract::<String>().ok());
    PositionMetaInput {
        asset_class,
        underlying,
        strike,
        expiry_days,
        option_type,
    }
}

fn py_datetime_to_secs(dt: &Bound<'_, PyAny>) -> PyResult<i64> {
    let ts = dt.call_method0("timestamp")?;
    let secs: f64 = ts.extract()?;
    Ok(secs as i64)
}

fn py_datetime_to_date_days(dt: &Bound<'_, PyAny>) -> PyResult<i64> {
    let ts = dt.call_method0("timestamp")?;
    let secs: f64 = ts.extract()?;
    Ok((secs / 86400.0).floor() as i64)
}

fn asset_class_to_string(ac: &Bound<'_, PyAny>) -> PyResult<String> {
    let name = ac.getattr("name")?.extract::<String>()?;
    Ok(name)
}

// --- Margin (Reg T style) ---

/// Compute used margin for a Rust portfolio. Same logic as Python RealisticRegTModel.
fn calculate_used_margin_rust(
    portfolio: &RustPortfolio,
    equity_initial: f64,
    _equity_maintenance: f64,
    option_base_pct: f64,
    option_min_pct: f64,
) -> f64 {
    let mut total_margin = 0.0;
    let positions = &portfolio.positions;
    let last_prices = &portfolio.last_prices;

    // Build underlying view: equity shares per symbol, option positions per underlying
    let mut underlying_options: HashMap<String, Vec<(String, &Position)>> = HashMap::new();
    for (sym, pos) in positions.iter() {
        if pos.asset_class.as_str() != "EQUITY" {
            let u = pos.underlying.as_deref().unwrap_or("");
            underlying_options.entry(u.to_string()).or_default().push((sym.clone(), pos));
        }
    }
    let all_underlyings: HashSet<String> = positions
        .keys()
        .filter(|s| positions.get(*s).map(|p| p.asset_class.as_str() == "EQUITY").unwrap_or(false))
        .cloned()
        .chain(underlying_options.keys().cloned())
        .collect();

    for underlying_sym in all_underlyings {
        let current_u_price = last_prices
            .get(&underlying_sym)
            .copied()
            .or_else(|| {
                positions
                    .get(&underlying_sym)
                    .map(|p| p.avg_cost)
            })
            .unwrap_or(0.0);
        let shares = positions
            .get(&underlying_sym)
            .filter(|p| p.asset_class.as_str() == "EQUITY")
            .map(|p| p.quantity)
            .unwrap_or(0.0);
        if shares != 0.0 {
            total_margin += shares.abs() * current_u_price * equity_initial;
        }
        let mut available_covered_shares = shares.max(0.0);
        let opt_list = underlying_options.get(&underlying_sym).map(|v| v.as_slice()).unwrap_or(&[]);
        for (opt_sym, opt_pos) in opt_list {
            let multiplier = opt_pos.multiplier;
            if opt_pos.quantity > 0.0 {
                continue;
            }
            let contracts = opt_pos.quantity.abs();
            let strike = opt_pos.strike.unwrap_or(0.0);
            let option_type = opt_pos.option_type.as_deref().unwrap_or("");
            if option_type == "C" {
                let mut contracts_to_margin = contracts;
                if available_covered_shares >= 100.0 {
                    let mult = multiplier as f64;
                    let covered = (contracts.min(available_covered_shares / mult)).min(contracts);
                    contracts_to_margin -= covered;
                    available_covered_shares -= covered * mult;
                }
                if contracts_to_margin > 0.0 {
                    let otm_amount = (strike - current_u_price).max(0.0);
                    let rule_1 = (option_base_pct * current_u_price) - otm_amount;
                    let rule_2 = option_min_pct * current_u_price;
                    let per_share_req = rule_1.max(rule_2);
                    total_margin += contracts_to_margin * multiplier * per_share_req;
                }
            } else if option_type == "P" {
                let otm_amount = (current_u_price - strike).max(0.0);
                let rule_1 = (option_base_pct * current_u_price) - otm_amount;
                let rule_2 = option_min_pct * strike;
                let per_share_req = rule_1.max(rule_2);
                total_margin += contracts * multiplier * per_share_req;
            }
        }
    }
    total_margin
}

#[pyfunction]
fn calculate_used_margin(
    portfolio: &RustPortfolio,
    equity_initial: f64,
    equity_maintenance: f64,
    option_base_pct: f64,
    option_min_pct: f64,
) -> f64 {
    calculate_used_margin_rust(
        portfolio,
        equity_initial,
        equity_maintenance,
        option_base_pct,
        option_min_pct,
    )
}

// --- Rust Backtest Engine ---

#[pyclass]
pub struct RustBacktestEngine {
    #[pyo3(get)]
    pub portfolio: RustPortfolio,
    history_ts: Vec<f64>,
    history_equity: Vec<f64>,
    history_cash: Vec<f64>,
}

#[pymethods]
impl RustBacktestEngine {
    #[new]
    pub fn new(initial_capital: f64) -> Self {
        RustBacktestEngine {
            portfolio: RustPortfolio::new(initial_capital),
            history_ts: Vec::new(),
            history_equity: Vec::new(),
            history_cash: Vec::new(),
        }
    }

    /// Run the backtest loop. `driver` must be a Python object with methods:
    /// - next_event() -> Optional[MarketEvent]
    /// - get_pending(symbol: str) -> list of (order, meta)
    /// - execute_order(order, event) -> Optional[FillEvent]
    /// - should_rebalance(event) -> bool
    /// - get_rebalance_orders(event) -> list of Order
    /// - get_strategy_orders(event) -> list of (order, meta)
    /// - add_pending(symbol: str, order, meta) -> None
    /// - set_history(ts: list, equity: list, cash: list) -> None
    pub fn run(&mut self, driver: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = driver.py();
        loop {
            let next_ev = driver.call_method0("next_event")?;
            if next_ev.is_none() {
                break;
            }
            let event = next_ev;

            self.portfolio.update_prices_from_event(&event)?;
            let ts: f64 = event.getattr("timestamp")?.call_method0("timestamp")?.extract()?;
            self.history_ts.push(ts);
            self.history_equity.push(self.portfolio.equity());
            self.history_cash.push(self.portfolio.cash);

            let sym: String = event.getattr("symbol")?.extract()?;

            let pending = driver.call_method1("get_pending", (sym.as_str(),))?;
            for item in pending.try_iter()? {
                let item: Bound<'_, PyAny> = item?;
                let order = item.get_item(0)?;
                let meta: Option<Bound<'_, PyAny>> = item.get_item(1).ok();
                let meta_bound = meta.as_ref().map(|m| m.clone()).unwrap_or_else(|| py.None().into_bound(py));
                let fill_result = driver.call_method1("execute_order", (order, meta_bound, &event))?;
                if !fill_result.is_none() {
                    self.portfolio.apply_fill_from_python(&fill_result, meta.as_ref())?;
                }
            }

            driver.call_method1("sync_portfolio", (self.portfolio.clone(),))?;

            let should_rebal: bool = driver.call_method1("should_rebalance", (&event,))?.extract()?;
            if should_rebal {
                let orders = driver.call_method1("get_rebalance_orders", (&event,))?;
                for order in orders.try_iter()? {
                    let order: Bound<'_, PyAny> = order?;
                    let order_sym: String = order.getattr("symbol")?.extract()?;
                    driver.call_method("add_pending", (order_sym, order, py.None()), None)?;
                }
            }

            let strategy_orders = driver.call_method1("get_strategy_orders", (&event,))?;
            for item in strategy_orders.try_iter()? {
                let item: Bound<'_, PyAny> = item?;
                let order = item.get_item(0)?;
                let meta = item.get_item(1).ok();
                let order_sym: String = order.getattr("symbol")?.extract()?;
                let meta_for_add = meta.as_ref().map(|m| m.clone()).unwrap_or_else(|| py.None().into_bound(py));
                driver.call_method("add_pending", (order_sym, order, meta_for_add), None)?;
            }
        }
        driver.call_method1(
            "set_history",
            (self.history_ts.clone(), self.history_equity.clone(), self.history_cash.clone()),
        )?;
        Ok(())
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Position>()?;
    m.add_class::<RustPortfolio>()?;
    m.add_class::<RustBacktestEngine>()?;
    m.add_function(wrap_pyfunction!(calculate_used_margin, m)?)?;
    Ok(())
}
