import streamlit as st
# Set page config first
st.set_page_config(page_title="Prédiction et Chatbot", layout="centered")

import pandas as pd
import joblib
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

# Paths for maintenance model
MODEL_PATH = "modelss/maintenance_model_model.pkl"
PREPROCESSOR_PATH = "modelss/maintenance_model_preprocessor.pkl"
LABEL_ENCODER_PATH = "modelss/maintenance_model_label_encoder.pkl"

# Path for fine-tuned T5 model
T5_MODEL_PATH = "./finetunedModels"

# Load maintenance model
@st.cache_resource
def load_maintenance_model():
    model = joblib.load(MODEL_PATH)
    preprocessor = joblib.load(PREPROCESSOR_PATH)
    label_encoder = joblib.load(LABEL_ENCODER_PATH)
    return model, preprocessor, label_encoder

# Load T5 chatbot model
@st.cache_resource
def load_chatbot_model():
    tokenizer = T5Tokenizer.from_pretrained(T5_MODEL_PATH)
    model = T5ForConditionalGeneration.from_pretrained(T5_MODEL_PATH)
    return tokenizer, model

# Load all models at startup
try:
    model, preprocessor, label_encoder = load_maintenance_model()
    tokenizer, t5_model = load_chatbot_model()
except Exception as e:
    st.error(f"Error loading models: {str(e)}")
    st.stop()

# Translation dictionary for predictions
french_predictions = {
    "No Failure": "Aucune Panne",
    "Heat Dissipation Failure": "Panne de Dissipation de Chaleur",
    "Power Failure": "Panne d'Alimentation",
    "Overstrain Failure": "Panne de Surcharge",
    "Tool Wear Failure": "Panne d'Usure d'Outil",
    "Random Failures": "Pannes Aléatoires"
}

# Recommendations dictionary
recommendations = {
    "No Failure": "La machine fonctionne normalement. Continuez l'entretien régulier.",
    "Heat Dissipation Failure": "Vérifiez le système de refroidissement et la circulation de l'air. Pensez à nettoyer les dissipateurs de chaleur.",
    "Power Failure": "Inspectez l'alimentation électrique et les connexions. Vérifiez les fluctuations de tension.",
    "Overstrain Failure": "Réduisez la charge sur la machine. Vérifiez s'il y a des obstructions mécaniques.",
    "Tool Wear Failure": "Remplacez ou affûtez les outils. Vérifiez l'alignement correct des outils.",
    "Random Failures": "Effectuez une inspection générale de tous les systèmes."
}

def generate_response(input_text):
    try:
        # Format the input - make it clear it's a question about maintenance
        formatted_input = f"Input: respond to this maintenance question: {input_text}"
        
        # Tokenize with padding
        inputs = tokenizer(
            formatted_input,
            return_tensors="pt",
            max_length=512,
            padding=True,
            truncation=True
        )
        
        # Generate with better parameters
        outputs = t5_model.generate(
            inputs.input_ids,
            max_length=150,  # Allow longer responses
            min_length=30,   # Ensure some minimum content
            num_beams=5,     # Increase beam search
            no_repeat_ngram_size=2,  # Avoid repetition
            top_k=50,        # More diverse outputs
            top_p=0.95,      # Nucleus sampling
            temperature=0.7,  # Add some randomness
            do_sample=True,  # Enable sampling
            early_stopping=True
        )
        
        # Decode the response
        response = tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # If response is just the input or too short, return a default message
        if response == input_text or len(response) < 10:
            return "Je ne peux pas générer une réponse appropriée à cette question. Veuillez reformuler votre question ou être plus spécifique concernant le problème de maintenance."
            
        return response
    except Exception as e:
        return f"Erreur lors de la génération de la réponse: {str(e)}"


# Sidebar for navigation
option = st.sidebar.radio("Navigation", ["Prédiction de la Panne", "Chatbot"])

if option == "Prédiction de la Panne":
    st.title("Prédiction de la Panne de Machine")
    st.subheader("Entrez les données de la machine pour prédire le type de défaillance")

    # User inputs
    type_ = st.selectbox('Type', ['L', 'M', 'H'])
    air_temperature = st.number_input('Température de l\'air [K]', min_value=0.0)
    process_temperature = st.number_input('Température du processus [K]', min_value=0.0)
    rotational_speed = st.number_input('Vitesse de rotation [rpm]', min_value=0.0)
    torque = st.number_input('Couple [Nm]', min_value=0.0)
    tool_wear = st.number_input('Usure de l\'outil [min]', min_value=0.0)

    if st.button('Prédire'):
        input_data = {
            'Type': [type_],
            'Air temperature [K]': [air_temperature],
            'Process temperature [K]': [process_temperature],
            'Rotational speed [rpm]': [rotational_speed],
            'Torque [Nm]': [torque],
            'Tool wear [min]': [tool_wear]
        }
        input_df = pd.DataFrame(input_data)
        
        try:
            input_processed = preprocessor.transform(input_df)
            prediction = model.predict(input_processed)
            predicted_failure = label_encoder.inverse_transform(prediction)[0]
            
            st.success(f"Type de panne prédite: {french_predictions.get(predicted_failure, 'Non défini')}")
            st.info(recommendations.get(predicted_failure, "Aucune recommandation disponible."))
            
        except Exception as e:
            st.error(f"Erreur lors de la prédiction: {str(e)}")

elif option == "Chatbot":
    st.title("Assistant de Maintenance")
    st.info("Posez vos questions sur la maintenance des machines ici.")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Votre question..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("En train de réfléchir..."):
                response = generate_response(prompt)
                st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})