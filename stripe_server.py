from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
import stripe
import os
import database  # Make sure this module handles a cloud DB (e.g., Supabase)
from dotenv import load_dotenv
from telegram import Bot # Import Bot to send confirmation messages
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

# Load environment variables (useful for local development, Render injects them directly)
load_dotenv() 

# Stripe configuration with environment variables
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
BOT_TOKEN = os.environ.get("BOT_TOKEN") # Make sure you have this value in Render

# Ensure Stripe keys are configured
if not stripe.api_key:
    logging.error("The STRIPE_SECRET_KEY environment variable is not configured.")
    raise ValueError("Stripe configuration incomplete: STRIPE_SECRET_KEY missing.")
if not STRIPE_WEBHOOK_SECRET:
    logging.error("The STRIPE_WEBHOOK_SECRET environment variable is not configured.")
    # Not a critical error for server startup, but necessary for secure webhooks.

# Bot instance to send confirmations (if BOT_TOKEN is available)
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
if not bot:
    logging.warning("BOT_TOKEN not configured in the Stripe backend. Confirmation messages cannot be sent to Telegram.")


# Define your point packages here with price in cents (USD)
# ⬅️ WE ADD 'priority_boost' to each package.
# LOWER 'priority_boost' values indicate HIGHER priority.
# Make sure this POINT_PACKAGES definition is synchronized with points_handlers.py in your bot
POINT_PACKAGES = {
    "p200": {"label": "2000 points", "amount": 399, "points": 2000, "priority_boost": 1},   # Normal Priority
    "p500": {"label": "5000 points", "amount": 999, "points": 5000, "priority_boost": 1},   # High Priority
    "p1000": {"label": "12000 points", "amount": 1999, "points": 12000, "priority_boost": 1} # Very High Priority
}

# Define el identificador único para este proyecto.
# Esto es crucial para el filtrado de webhooks.
PROJECT_IDENTIFIER = "monkeynudesbot" # <--- IDENTIFICADOR ÚNICO PARA ESTE PROYECTO

@app.post("/crear-sesion")
async def crear_sesion(request: Request):
    """
    Endpoint to create a Stripe checkout session.
    Called from your Telegram bot.
    """
    data = await request.json()
    id = str(data.get("telegram_id"))
    paquete_id = data.get("paquete_id")
    # ⬅️ We receive the priority_boost from the bot
    priority_boost = data.get("priority_boost") 

    # Validation
    if not id or paquete_id not in POINT_PACKAGES:
        logging.error(f"Invalid data in /crear-sesion: id={id}, paquete_id={paquete_id}")
        return JSONResponse(status_code=400, content={"error": "Invalid data: incorrect id or package_id."})
    
    # Validate that priority_boost is a valid integer if sent
    if priority_boost is not None and not isinstance(priority_boost, int):
        logging.error(f"Invalid data type for priority_boost: {priority_boost}")
        return JSONResponse(status_code=400, content={"error": "Invalid data: priority_boost must be an integer."})

    paquete = POINT_PACKAGES[paquete_id]

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "unit_amount": paquete["amount"],
                    "product_data": {
                        "name": paquete["label"]
                    }
                },
                "quantity": 1
            }],
            mode="payment",
            success_url="https://t.me/monkeynudesbot",   # URL de éxito para este bot
            cancel_url="https://t.me/monkeynudesbot",    # URL de cancelación para este bot
            metadata={
                "telegram_id": id,
                "package_id": paquete_id,
                "points_awarded": paquete["points"], # Also useful for the webhook
                "priority_boost": priority_boost,    # ⬅️ We pass the priority_boost in the metadata
                "project": PROJECT_IDENTIFIER        # <--- AÑADIDO: Identificador del proyecto
            }
        )
        logging.info(f"Stripe session created for user {id}, package {paquete_id}. URL: {session.url}")
        return {"url": session.url}
    except Exception as e:
        logging.error(f"Error creating Stripe session for user {id}, package {paquete_id}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": f"Internal error creating session: {str(e)}"})

@app.post("/webhook/stripe")
async def stripe_webhook(request: Request, stripe_signature: str = Header(None, alias="Stripe-Signature")):
    """
    Endpoint that receives Stripe webhooks.
    It is called by Stripe when events like 'checkout.session.completed' occur.
    """
    payload = await request.body()

    try:
        event = stripe.Webhook.construct_event(payload, stripe_signature, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError as e:
        logging.error(f"Stripe webhook signature verification error: {e}")
        raise HTTPException(status_code=400, detail="Invalid signature")
    except ValueError as e:
        logging.error(f"Stripe webhook payload processing error: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")
    
    # --- INICIO DE LA LÓGICA DE FILTRADO POR METADATA ---
    # Si el evento es de tipo 'checkout.session.completed', verificamos el metadata 'project'.
    # Si el evento no tiene el metadata 'project' o no coincide con este backend, lo ignoramos.
    if event["type"] == "checkout.session.completed":
        session_metadata = event["data"]["object"].get("metadata", {})
        event_project = session_metadata.get("project")

        if event_project != PROJECT_IDENTIFIER:
            logging.info(f"Webhook received for project '{event_project}', but this backend is '{PROJECT_IDENTIFIER}'. Ignoring event.")
            return JSONResponse(status_code=200, content={"status": "ignored", "reason": "project_mismatch"})
    # --- FIN DE LA LÓGICA DE FILTRADO POR METADATA ---

    # Handle checkout session completed event
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        metadata = session.get("metadata", {})
        id_str = metadata.get("telegram_id") # Read as string
        package_id = metadata.get("package_id")
        points_awarded = metadata.get("points_awarded") # Points to award
        priority_boost = metadata.get("priority_boost") # ⬅️ Retrieve the priority_boost

        # Safely convert id to int
        try:
            id = int(id_str)
        except (ValueError, TypeError):
            logging.error(f"Webhook: Invalid or missing id in metadata: {id_str}")
            return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid id in metadata"})

        # Safely convert points_awarded to int
        try:
            points_awarded = int(points_awarded)
        except (ValueError, TypeError):
            logging.error(f"Webhook: Invalid or missing points_awarded in metadata: {points_awarded}")
            points_awarded = 0 # Or handle as error if critical

        # Safely convert priority_boost to int
        try:
            priority_boost = int(priority_boost)
        except (ValueError, TypeError):
            logging.warning(f"Webhook: Invalid or missing priority_boost in metadata: {priority_boost}. Using default priority (2).")
            priority_boost = 2 # Use default priority if it cannot be converted

        if id is not None and package_id in POINT_PACKAGES:
            try:
                # Update user points
                # Asegúrate de que tu database.py para Monkeyhentai usa la tabla correcta (ej. users_image)
                database.update_user_points(id, points_awarded) 
                logging.info(f"User {id} received {points_awarded} points for Stripe purchase.")

                # ⬅️ Update user priority
                # We only update if the new priority is "better" (numerically lower)
                database.update_user_priority(id, priority_boost)
                logging.info(f"User {id} priority updated to {priority_boost} (if better).")

                # Send confirmation message to Telegram user
                if bot: # Only try to send if the bot was initialized correctly
                    try:
                        await bot.send_message(
                            chat_id=id,
                            text=f"🎉 **Recharge successful!** <b>{points_awarded}</b> points have been added to your account. Your queue priority is now <b>{priority_boost}</b> (0=Highest).",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logging.error(f"Error sending Telegram confirmation message for {id}: {e}")
                else:
                    logging.warning("Warning: Telegram bot not initialized in Stripe backend (TOKEN missing?). Could not send confirmation.")
            except Exception as e:
                logging.error(f"Error updating points/priority or sending confirmation for {id}: {e}", exc_info=True)
        else:
            logging.warning(f"Webhook received but incomplete or invalid metadata: id={id_str}, package_id={package_id}")

    # You can handle other Stripe event types here if needed
    # elif event["type"] == "payment_intent.succeeded":
    #     logging.info("Payment Intent succeeded!")

    return JSONResponse(status_code=200, content={"status": "ok"})
