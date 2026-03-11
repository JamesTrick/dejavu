from dataclasses import dataclass

from dejavu.portfolio import Portfolio


@dataclass
class MarginConfig:
    equity_initial: float = 0.50
    equity_maintenance: float = 0.25
    option_base_pct: float = 0.20
    option_min_pct: float = 0.10


class RealisticRegTModel:
    def __init__(self, config: MarginConfig = MarginConfig()):
        self.config = config

    def calculate_used_margin(self, portfolio: Portfolio) -> float:
        total_margin = 0.0
        view = portfolio.underlying_view()

        for underlying_sym, data in view.items():
            current_u_price = data["price"]
            equity_pos = data["equity_position"]
            opt_symbols = data["option_symbols"]

            # 1. Equity Margin
            shares = equity_pos.quantity if equity_pos else 0.0
            if shares != 0:
                total_margin += abs(shares) * current_u_price * self.config.equity_initial

            # 2. Track covered shares to offset Call margin
            available_covered_shares = max(0.0, shares)

            for opt_sym in opt_symbols:
                opt_pos = portfolio.positions[opt_sym]
                multiplier = getattr(opt_pos, 'multiplier', 100.0)

                # Long options require no ongoing margin (paid upfront in cash)
                if opt_pos.quantity > 0:
                    continue

                # Short Options Logic
                contracts = abs(opt_pos.quantity)
                strike = opt_pos.strike or 0.0

                if opt_pos.option_type == "C":
                    # Check if covered by stock
                    contracts_to_margin = contracts
                    if available_covered_shares >= 100:
                        covered_contracts = min(contracts, available_covered_shares // multiplier)
                        contracts_to_margin -= covered_contracts
                        available_covered_shares -= (covered_contracts * multiplier)

                    if contracts_to_margin > 0:
                        # Naked Call Reg T Formula
                        otm_amount = max(0.0, strike - current_u_price)
                        rule_1 = (self.config.option_base_pct * current_u_price) - otm_amount
                        rule_2 = self.config.option_min_pct * current_u_price

                        per_share_req = max(rule_1, rule_2)
                        total_margin += contracts_to_margin * multiplier * per_share_req

                elif opt_pos.option_type == "P":
                    # Naked Put Reg T Formula (assuming no short stock offsets for simplicity here)
                    otm_amount = max(0.0, current_u_price - strike)
                    rule_1 = (self.config.option_base_pct * current_u_price) - otm_amount
                    rule_2 = self.config.option_min_pct * strike  # Puts use 10% of strike, not underlying

                    per_share_req = max(rule_1, rule_2)
                    total_margin += contracts * multiplier * per_share_req

        return total_margin
