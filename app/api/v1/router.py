from fastapi import APIRouter
from app.api.v1 import suppliers, customers, service_points, contracts, commissions, uploads, reconciliation, dashboard, crm, leads, tasks, calllist, proposals, ai_agent
from app.api.v1 import auth as auth_module, users as users_module

router = APIRouter(prefix="/api/v1")

router.include_router(auth_module.router, prefix="/auth", tags=["Auth"])
router.include_router(users_module.router, prefix="/users", tags=["Users"])
router.include_router(suppliers.router, prefix="/suppliers", tags=["Suppliers"])
router.include_router(customers.router, prefix="/customers", tags=["Customers"])
router.include_router(service_points.router, prefix="/service-points", tags=["Service Points"])
router.include_router(contracts.router, prefix="/contracts", tags=["Contracts"])
router.include_router(commissions.router, prefix="/commissions", tags=["Commissions"])
router.include_router(uploads.router, prefix="/uploads", tags=["Uploads"])
router.include_router(reconciliation.router, prefix="/reconciliation", tags=["Reconciliation"])
router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
router.include_router(crm.router, prefix="/crm", tags=["CRM"])
router.include_router(leads.router, prefix="/leads", tags=["Leads"])
router.include_router(tasks.router, prefix="/tasks", tags=["Tasks"])
router.include_router(calllist.router, prefix="/call-list", tags=["Call List"])
router.include_router(proposals.router, prefix="/proposals", tags=["Proposals"])
router.include_router(ai_agent.router, prefix="/ai-agent", tags=["AI Agent"])
