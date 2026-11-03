from app.services.adder_visibility import can_see_adder, redact_adder


class U:
    def __init__(self, role, name, agent=None):
        self.role, self.name, self.sales_agent_name = role, name, agent
    @property
    def is_admin(self): return self.role == "admin"


def test_admin_sees_everything():
    u = U("admin", "Lance Nguyen")
    assert can_see_adder(u, "Anyone")
    deals = [{"sales_agent": "Nga Nguyen", "adder": 0.007}]
    redact_adder(u, deals)
    assert deals[0]["adder"] == 0.007


def test_sales_agent_sees_only_own_deals():
    u = U("sales_agent", "Nga N.", agent="Nga Nguyen")
    assert can_see_adder(u, "nga nguyen ")
    assert not can_see_adder(u, "Jennie Tran")
    deals = [{"sales_agent": "Nga Nguyen", "adder": 0.007}, {"sales_agent": "Jennie Tran", "adder": 0.008}]
    redact_adder(u, deals)
    assert deals[0]["adder"] == 0.007 and "adder" not in deals[1]


def test_manager_and_csr_see_nothing_unless_own():
    u = U("manager", "Jennie Tran")
    deals = [{"sales_agent": "Jennie Tran", "adder": 0.01}, {"sales_agent": "Nga Nguyen", "adder": 0.007}, {"adder": 0.005}]
    redact_adder(u, deals)
    assert deals[0]["adder"] == 0.01 and "adder" not in deals[1] and "adder" not in deals[2]
