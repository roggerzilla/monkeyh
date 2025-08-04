import os
import logging
import json
from datetime import datetime

from supabase import create_client, Client
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logging.error("Las variables de entorno SUPABASE_URL o SUPABASE_KEY no están configuradas.")
    raise ValueError("Configuración de Supabase incompleta.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Funciones para la tabla 'users' ---
def get_user(id: int):
    """Obtiene datos de un usuario por su ID de Telegram."""
    try:
        response = supabase.table("users_image").select("*").eq("id", id).execute()
        data = response.data
        return data[0] if data else None
    except Exception as e:
        logging.error(f"Error al obtener usuario {id}: {e}")
        return None

def add_user(id: int, referred_by=None, initial_points=0):
    """Añade un nuevo usuario a la base de datos con puntos iniciales y prioridad por defecto."""
    user = get_user(id)
    if user:
        logging.warning(f"Usuario {id} ya existe. Saltando adición.")
        return False

    data = {
        "id": id,
        "points": initial_points,
        "referred_by": referred_by,
        "priority": 2  # CORREGIDO: Se usa 'priority'
    }
    try:
        response = supabase.table("users_image").insert(data).execute()
        if response.data:
            logging.info(f"Usuario {id} añadido a la BD. Puntos: {initial_points}, Prioridad: 2.")
            return True
        logging.warning(f"No se pudo añadir usuario {id}: {response.json()}.")
        return False
    except Exception as e:
        logging.warning(f"Error al añadir usuario {id} (puede que ya exista): {e}.")
        return False

def update_user_points(id: int, amount: int):
    """Actualiza los puntos de un usuario."""
    user = get_user(id)
    if not user:
        logging.warning(f"Usuario {id} no encontrado para actualizar puntos.")
        return None

    new_points = user["points"] + amount
    try:
        response = supabase.table("users_image").update({"points": new_points}).eq("id", id).execute()
        if response.data:
            logging.info(f"Puntos de usuario {id} actualizados en {amount} (total: {new_points}).")
            return response.data[0]
        logging.error(f"Error al actualizar puntos para el usuario {id}: {response.json()}.")
        return None
    except Exception as e:
        logging.error(f"Error al actualizar puntos para el usuario {id}: {e}.")
        return None

def get_user_points(id: int) -> int:
    """Obtiene el saldo actual de puntos de un usuario."""
    user = get_user(id)
    return user["points"] if user else 0

def get_user_priority(id: int) -> int:
    """Obtiene el nivel de prioridad actual de un usuario."""
    user = get_user(id)
    return user.get("priority", 2) if user else 2  # CORREGIDO: Se usa 'priority'

def update_user_priority(id: int, new_priority_level: int):
    """Actualiza el nivel de prioridad de un usuario."""
    user = get_user(id)
    if not user:
        logging.warning(f"Usuario {id} no encontrado para actualizar prioridad.")
        return False

    current_priority = user.get("priority", 2)  # CORREGIDO: Se usa 'priority'
    
    if new_priority_level < current_priority:
        try:
            response = supabase.table("users_image").update({'priority': new_priority_level}).eq('id', id).execute()  # CORREGIDO: Se usa 'priority'
            if response.data:
                logging.info(f"Prioridad del usuario {id} actualizada de {current_priority} a {new_priority_level}.")
                return True
            logging.error(f"Error al actualizar prioridad del usuario {id}: {response.json()}.")
            return False
        except Exception as e:
            logging.error(f"Error al actualizar prioridad del usuario {id}: {e}.")
            return False
    else:
        logging.info(f"La nueva prioridad {new_priority_level} no es mejor que la actual {current_priority} para el usuario {id}.")
        return False

# --- Funciones para la tabla 'generation_queue' ---
async def add_generation_job(user_id: int, chat_id: int, message_id: int, filepath: str, workflow_content: dict, selected_workflow_name: str, priority_level: int):
    """
    Añade un trabajo de generación a la cola persistente en Supabase.
    """
    job_data = {
        'user_id': user_id,
        'chat_id': chat_id,
        'message_id': message_id,
        'filepath': filepath,
        'workflow_content': json.dumps(workflow_content),
        'selected_workflow_name': selected_workflow_name,
        'status': 'pending',
        'priority_level': priority_level  # CORRECTO: Se mantiene 'priority_level'
    }
    try:
        response = supabase.table("generation_queue_image").insert(job_data).execute()
        if response.data:
            logging.info(f"Trabajo de generación para {user_id} añadido. ID del trabajo: {response.data[0]['id']}.")
            return response.data[0]['id']
        logging.error(f"Error al añadir trabajo de generación: {response.json()}.")
        return None
    except Exception as e:
        logging.error(f"Error al añadir trabajo de generación para usuario {user_id}: {e}.")
        return None
        
async def get_next_generation_job():
    """Obtiene el siguiente trabajo de la cola con la prioridad más alta."""
    try:
        response = supabase.table("generation_queue_image") \
            .select('*') \
            .eq('status', 'pending') \
            .order('priority_level', asc=True) \
            .order('created_at', asc=True) \
            .limit(1) \
            .execute()

        if not response.data:
            return None

        job = response.data[0]
        job_id = job['id']

        update_response = supabase.table("generation_queue_image") \
            .update({'status': 'processing', 'started_at': datetime.now().isoformat()}) \
            .eq('id', job_id) \
            .eq('status', 'pending') \
            .execute()

        if update_response.data:
            logging.info(f"Trabajo {job_id} marcado como 'processing'.")
            job['workflow_content'] = json.loads(job['workflow_content'])
            return job
        else:
            logging.warning(f"Trabajo {job_id} ya fue tomado o su estado cambió. Reintentando la búsqueda...")
            return None

    except Exception as e:
        logging.error(f"Error al obtener o marcar trabajo de generación en cola: {e}.")
        return None

async def update_generation_job_status(job_id: str, status: str, output_files_urls: list = None, error_message: str = None):
    """Actualiza el estado de un trabajo de generación en la cola persistente."""
    update_data = {'status': status}
    if status == 'completed':
        update_data['completed_at'] = datetime.now().isoformat()
        if output_files_urls:
            update_data['output_files_urls'] = json.dumps(output_files_urls)
    elif status in ('failed', 'refunded', 'canceled'):
        update_data['error_message'] = error_message
        update_data['completed_at'] = datetime.now().isoformat()

    try:
        response = supabase.table("generation_queue_image").update(update_data).eq('id', job_id).execute()
        if response.data:
            logging.info(f"Estado del trabajo {job_id} actualizado a {status}.")
        else:
            logging.error(f"Error al actualizar estado del trabajo {job_id}: {response.json()}.")
    except Exception as e:
        logging.error(f"Error en update_generation_job_status para {job_id}: {e}.")

async def get_uncompleted_processing_jobs():
    """Recupera trabajos que quedaron en estado 'processing' de una sesión anterior."""
    try:
        response = supabase.table("generation_queue_image") \
            .select('id, user_id, chat_id, filepath, selected_workflow_name') \
            .eq('status', 'processing') \
            .execute()
        
        if response.data:
            logging.warning(f"Encontrados {len(response.data)} trabajos en estado 'processing' no completados tras un reinicio.")
        return response.data
    except Exception as e:
        logging.error(f"Error al recuperar trabajos 'processing' no completados: {e}.")
        return []