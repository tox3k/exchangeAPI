from fastapi import FastAPI, HTTPException, Depends, Header, status
from fastapi.security import HTTPBearer
from pydantic import BaseModel, UUID4, conint, constr
from typing import List, Optional, Dict, Union
from uuid import uuid4
from datetime import datetime
from enum import Enum
import databases
import sqlalchemy
from sqlalchemy import and_, or_

# Database setup
DATABASE_URL = "sqlite:///./market.db"
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

# Models
users = sqlalchemy.Table(
    "users",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String(36), primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String(50)),
    sqlalchemy.Column("role", sqlalchemy.String(10)),
    sqlalchemy.Column("api_key", sqlalchemy.String(50)),
)

instruments = sqlalchemy.Table(
    "instruments",
    metadata,
    sqlalchemy.Column("name", sqlalchemy.String(50)),
    sqlalchemy.Column("ticker", sqlalchemy.String(10), primary_key=True),
)

balances = sqlalchemy.Table(
    "balances",
    metadata,
    sqlalchemy.Column("user_id", sqlalchemy.String(36)),
    sqlalchemy.Column("ticker", sqlalchemy.String(10)),
    sqlalchemy.Column("amount", sqlalchemy.Integer),
    sqlalchemy.PrimaryKeyConstraint("user_id", "ticker"),
)

orders = sqlalchemy.Table(
    "orders",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String(36), primary_key=True),
    sqlalchemy.Column("status", sqlalchemy.String(20)),
    sqlalchemy.Column("user_id", sqlalchemy.String(36)),
    sqlalchemy.Column("timestamp", sqlalchemy.DateTime),
    sqlalchemy.Column("direction", sqlalchemy.String(4)),
    sqlalchemy.Column("ticker", sqlalchemy.String(10)),
    sqlalchemy.Column("qty", sqlalchemy.Integer),
    sqlalchemy.Column("price", sqlalchemy.Integer, nullable=True),
    sqlalchemy.Column("filled", sqlalchemy.Integer, default=0),
    sqlalchemy.Column("order_type", sqlalchemy.String(10)),
)

transactions = sqlalchemy.Table(
    "transactions",
    metadata,
    sqlalchemy.Column("ticker", sqlalchemy.String(10)),
    sqlalchemy.Column("amount", sqlalchemy.Integer),
    sqlalchemy.Column("price", sqlalchemy.Integer),
    sqlalchemy.Column("timestamp", sqlalchemy.DateTime),
    sqlalchemy.Column("buyer_id", sqlalchemy.String(36)),
    sqlalchemy.Column("seller_id", sqlalchemy.String(36)),
)

engine = sqlalchemy.create_engine(DATABASE_URL)
metadata.create_all(engine)

app = FastAPI(title="Toy Exchange", version="0.1.0")

# Security
security = HTTPBearer()

# Enums
class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(str, Enum):
    NEW = "NEW"
    EXECUTED = "EXECUTED"
    PARTIALLY_EXECUTED = "PARTIALLY_EXECUTED"
    CANCELLED = "CANCELLED"

class UserRole(str, Enum):
    USER = "USER"
    ADMIN = "ADMIN"

# Pydantic models
class NewUser(BaseModel):
    name: constr(min_length=3)

class User(BaseModel):
    id: UUID4
    name: str
    role: UserRole
    api_key: str

class Instrument(BaseModel):
    name: str
    ticker: constr(pattern=r'^[A-Z]{2,10}$')

class Level(BaseModel):
    price: int
    qty: int

class L2OrderBook(BaseModel):
    bid_levels: List[Level]
    ask_levels: List[Level]

class Transaction(BaseModel):
    ticker: str
    amount: int
    price: int
    timestamp: datetime

class LimitOrderBody(BaseModel):
    direction: Direction
    ticker: str
    qty: conint(gt=0)
    price: conint(gt=0)

class MarketOrderBody(BaseModel):
    direction: Direction
    ticker: str
    qty: conint(gt=0)

class CreateOrderResponse(BaseModel):
    success: bool = True
    order_id: UUID4

class Ok(BaseModel):
    success: bool = True

class DepositWithdrawBody(BaseModel):
    user_id: UUID4
    ticker: str
    amount: conint(gt=0)

# Dependency to get current user
async def get_current_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("TOKEN "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header"
        )
    
    api_key = authorization[6:]
    query = users.select().where(users.c.api_key == api_key)
    user = await database.fetch_one(query)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    
    return user

# Dependency to check admin role
async def get_admin_user(user: dict = Depends(get_current_user)):
    if user["role"] != "ADMIN":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return user

# Startup event
@app.on_event("startup")
async def startup():
    await database.connect()
    # Add some initial data if needed
    # await database.execute(users.insert().values(id=str(uuid4()), name="Admin", role="ADMIN", api_key="admin-key"))

# Shutdown event
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# Public endpoints
@app.post("/api/v1/public/register", response_model=User, tags=["public"])
async def register(new_user: NewUser):
    user_id = str(uuid4())
    api_key = f"key-{str(uuid4())}"
    
    query = users.insert().values(
        id=user_id,
        name=new_user.name,
        role="USER",
        api_key=api_key
    )
    await database.execute(query)
    
    return {
        "id": user_id,
        "name": new_user.name,
        "role": "USER",
        "api_key": api_key
    }

@app.get("/api/v1/public/instrument", response_model=List[Instrument], tags=["public"])
async def list_instruments():
    query = instruments.select()
    return await database.fetch_all(query)

@app.get("/api/v1/public/orderbook/{ticker}", response_model=L2OrderBook, tags=["public"])
async def get_orderbook(ticker: str, limit: int = 10):
    if limit > 25:
        limit = 25
    
    # Get bids (BUY orders)
    bid_query = orders.select().where(
        and_(
            orders.c.ticker == ticker,
            orders.c.direction == "BUY",
            or_(
                orders.c.status == "NEW",
                orders.c.status == "PARTIALLY_EXECUTED"
            )
        )
    ).order_by(orders.c.price.desc()).limit(limit)
    bids = await database.fetch_all(bid_query)
    
    # Get asks (SELL orders)
    ask_query = orders.select().where(
        and_(
            orders.c.ticker == ticker,
            orders.c.direction == "SELL",
            or_(
                orders.c.status == "NEW",
                orders.c.status == "PARTIALLY_EXECUTED"
            )
        )
    ).order_by(orders.c.price.asc()).limit(limit)
    asks = await database.fetch_all(ask_query)
    
    # Aggregate levels
    bid_levels = {}
    for bid in bids:
        if bid["price"] in bid_levels:
            bid_levels[bid["price"]] += bid["qty"] - bid["filled"]
        else:
            bid_levels[bid["price"]] = bid["qty"] - bid["filled"]
    
    ask_levels = {}
    for ask in asks:
        if ask["price"] in ask_levels:
            ask_levels[ask["price"]] += ask["qty"] - ask["filled"]
        else:
            ask_levels[ask["price"]] = ask["qty"] - ask["filled"]
    
    return {
        "bid_levels": [{"price": p, "qty": q} for p, q in bid_levels.items()],
        "ask_levels": [{"price": p, "qty": q} for p, q in ask_levels.items()]
    }

@app.get("/api/v1/public/transactions/{ticker}", response_model=List[Transaction], tags=["public"])
async def get_transaction_history(ticker: str, limit: int = 10):
    if limit > 100:
        limit = 100
    
    query = transactions.select().where(
        transactions.c.ticker == ticker
    ).order_by(transactions.c.timestamp.desc()).limit(limit)
    return await database.fetch_all(query)

# Balance endpoints
@app.get("/api/v1/balance", response_model=Dict[str, int], tags=["balance"])
async def get_balances(user: dict = Depends(get_current_user)):
    query = balances.select().where(balances.c.user_id == str(user["id"]))
    balance_records = await database.fetch_all(query)
    return {b["ticker"]: b["amount"] for b in balance_records}

# Order endpoints
@app.post("/api/v1/order", response_model=CreateOrderResponse, tags=["order"])
async def create_order(
    order_body: Union[LimitOrderBody, MarketOrderBody],
    user: dict = Depends(get_current_user)
):
    order_id = str(uuid4())
    order_type = "LIMIT" if isinstance(order_body, LimitOrderBody) else "MARKET"
    
    # For market orders, we need to execute immediately
    if order_type == "MARKET":
        # Find matching orders
        opposite_direction = "SELL" if order_body.direction == "BUY" else "BUY"
        
        query = orders.select().where(
            and_(
                orders.c.ticker == order_body.ticker,
                orders.c.direction == opposite_direction,
                or_(
                    orders.c.status == "NEW",
                    orders.c.status == "PARTIALLY_EXECUTED"
                )
            )
        ).order_by(orders.c.price.asc() if order_body.direction == "BUY" else orders.c.price.desc())
        
        matching_orders = await database.fetch_all(query)
        
        remaining_qty = order_body.qty
        executed = False
        
        for match in matching_orders:
            if remaining_qty <= 0:
                break
                
            match_qty_available = match["qty"] - match["filled"]
            execution_qty = min(remaining_qty, match_qty_available)
            execution_price = match["price"]
            
            # Record transaction
            transaction_query = transactions.insert().values(
                ticker=order_body.ticker,
                amount=execution_qty,
                price=execution_price,
                timestamp=datetime.now(),
                buyer_id=user["id"] if order_body.direction == "BUY" else match["user_id"],
                seller_id=match["user_id"] if order_body.direction == "BUY" else user["id"]
            )
            await database.execute(transaction_query)
            
            # Update matched order
            new_filled = match["filled"] + execution_qty
            new_status = "EXECUTED" if new_filled >= match["qty"] else "PARTIALLY_EXECUTED"
            
            update_match_query = orders.update().where(
                orders.c.id == match["id"]
            ).values(
                filled=new_filled,
                status=new_status
            )
            await database.execute(update_match_query)
            
            # Update balances
            # For BUY: user gets qty, loses qty*price
            # For SELL: user loses qty, gets qty*price
            ticker = order_body.ticker
            base_currency = "MEMCOIN"  # Assuming MEMCOIN is the base currency
            
            if order_body.direction == "BUY":
                # Update buyer balance (receive ticker, pay base currency)
                await update_balance(user["id"], ticker, execution_qty)
                await update_balance(user["id"], base_currency, -execution_qty * execution_price)
                
                # Update seller balance (receive base currency, pay ticker)
                await update_balance(match["user_id"], base_currency, execution_qty * execution_price)
                await update_balance(match["user_id"], ticker, -execution_qty)
            else:
                # Update seller balance (receive base currency, pay ticker)
                await update_balance(user["id"], base_currency, execution_qty * execution_price)
                await update_balance(user["id"], ticker, -execution_qty)
                
                # Update buyer balance (receive ticker, pay base currency)
                await update_balance(match["user_id"], ticker, execution_qty)
                await update_balance(match["user_id"], base_currency, -execution_qty * execution_price)
            
            remaining_qty -= execution_qty
            executed = True
        
        if remaining_qty > 0 and executed:
            # Partially filled
            status = "PARTIALLY_EXECUTED"
        elif remaining_qty > 0:
            # No matches found
            status = "NEW"
        else:
            # Fully executed
            status = "EXECUTED"
        
        # For market orders, we don't store them if fully executed
        if status == "EXECUTED":
            return {"success": True, "order_id": order_id}
        
        # Store partially executed or unmatched market order
        insert_query = orders.insert().values(
            id=order_id,
            status=status,
            user_id=user["id"],
            timestamp=datetime.now(),
            direction=order_body.direction,
            ticker=order_body.ticker,
            qty=order_body.qty,
            price=None,
            filled=order_body.qty - remaining_qty,
            order_type="MARKET"
        )
        await database.execute(insert_query)
        
        return {"success": True, "order_id": order_id}
    
    else:  # LIMIT order
        # Just store the order
        insert_query = orders.insert().values(
            id=order_id,
            status="NEW",
            user_id=user["id"],
            timestamp=datetime.now(),
            direction=order_body.direction,
            ticker=order_body.ticker,
            qty=order_body.qty,
            price=order_body.price,
            filled=0,
            order_type="LIMIT"
        )
        await database.execute(insert_query)
        
        return {"success": True, "order_id": order_id}

@app.get("/api/v1/order", response_model=List[Union[LimitOrderBody, MarketOrderBody]], tags=["order"])
async def list_orders(user: dict = Depends(get_current_user)):
    query = orders.select().where(orders.c.user_id == user["id"])
    return await database.fetch_all(query)

@app.get("/api/v1/order/{order_id}", response_model=Union[LimitOrderBody, MarketOrderBody], tags=["order"])
async def get_order(order_id: str, user: dict = Depends(get_current_user)):
    query = orders.select().where(
        and_(
            orders.c.id == order_id,
            orders.c.user_id == user["id"]
        )
    )
    order = await database.fetch_one(query)
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return order

@app.delete("/api/v1/order/{order_id}", response_model=Ok, tags=["order"])
async def cancel_order(order_id: str, user: dict = Depends(get_current_user)):
    # Check if order exists and belongs to user
    query = orders.select().where(
        and_(
            orders.c.id == order_id,
            orders.c.user_id == user["id"]
        )
    )
    order = await database.fetch_one(query)
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if order["status"] in ("EXECUTED", "CANCELLED"):
        raise HTTPException(status_code=400, detail="Order cannot be cancelled")
    
    # Update order status
    update_query = orders.update().where(
        orders.c.id == order_id
    ).values(status="CANCELLED")
    await database.execute(update_query)
    
    return {"success": True}

# Admin endpoints
@app.delete("/api/v1/admin/user/{user_id}", response_model=User, tags=["admin", "user"])
async def delete_user(user_id: str, admin: dict = Depends(get_admin_user)):
    # Get user first
    query = users.select().where(users.c.id == user_id)
    user = await database.fetch_one(query)
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Delete user
    delete_query = users.delete().where(users.c.id == user_id)
    await database.execute(delete_query)
    
    # Also delete their balances and orders
    await database.execute(balances.delete().where(balances.c.user_id == user_id))
    await database.execute(orders.delete().where(orders.c.user_id == user_id))
    
    return user

@app.post("/api/v1/admin/instrument", response_model=Ok, tags=["admin"])
async def add_instrument(instrument: Instrument, admin: dict = Depends(get_admin_user)):
    # Check if instrument already exists
    query = instruments.select().where(instruments.c.ticker == instrument.ticker)
    existing = await database.fetch_one(query)
    
    if existing:
        raise HTTPException(status_code=400, detail="Instrument already exists")
    
    # Add new instrument
    insert_query = instruments.insert().values(
        name=instrument.name,
        ticker=instrument.ticker
    )
    await database.execute(insert_query)
    
    return {"success": True}

@app.delete("/api/v1/admin/instrument/{ticker}", response_model=Ok, tags=["admin"])
async def delete_instrument(ticker: str, admin: dict = Depends(get_admin_user)):
    # Check if instrument exists
    query = instruments.select().where(instruments.c.ticker == ticker)
    existing = await database.fetch_one(query)
    
    if not existing:
        raise HTTPException(status_code=404, detail="Instrument not found")
    
    # Delete instrument
    delete_query = instruments.delete().where(instruments.c.ticker == ticker)
    await database.execute(delete_query)
    
    # Also delete related orders and transactions
    await database.execute(orders.delete().where(orders.c.ticker == ticker))
    await database.execute(transactions.delete().where(transactions.c.ticker == ticker))
    
    return {"success": True}

@app.post("/api/v1/admin/balance/deposit", response_model=Ok, tags=["admin", "balance"])
async def deposit(body: DepositWithdrawBody, admin: dict = Depends(get_admin_user)):
    await update_balance(body.user_id, body.ticker, body.amount)
    return {"success": True}

@app.post("/api/v1/admin/balance/withdraw", response_model=Ok, tags=["admin", "balance"])
async def withdraw(body: DepositWithdrawBody, admin: dict = Depends(get_admin_user)):
    await update_balance(body.user_id, body.ticker, -body.amount)
    return {"success": True}

# Helper functions
async def update_balance(user_id: str, ticker: str, amount: int):
    # Check current balance
    # print(user_id)
    # print(ticker)
    # for r in await database.fetch_all(f'SELECT * FROM balances WHERE user_id = "{user_id}"" AND ticker = "{ticker}"'):
    #     print("-------------")
    #     print(r["user_id"])
    #     print(r["ticker"])
    #     print(r["amount"])
    #     print("-------------")

    query = balances.select().where(
        and_(
            balances.c.user_id == str(user_id),
            balances.c.ticker == ticker
        )
    )
    
    balance = await database.fetch_one(query)
    print(balance)
    if balance:
        new_amount = balance["amount"] + amount
        if new_amount < 0:
            raise HTTPException(status_code=400, detail="Insufficient funds")
        
        update_query = balances.update().where(
            and_(
                balances.c.user_id == str(user_id),
                balances.c.ticker == ticker
            )
        ).values(amount=new_amount)
        await database.execute(update_query)
    else:
        if amount < 0:
            raise HTTPException(status_code=400, detail="Insufficient funds")
        
        insert_query = balances.insert().values(
            user_id=str(user_id),
            ticker=ticker,
            amount=amount
        )
        await database.execute(insert_query)
