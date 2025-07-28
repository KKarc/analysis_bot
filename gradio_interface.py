import gradio as gr
import sys

# Attempt to import the analiser module.
# The analiser.py script will execute its top-level code upon import,
# which includes loading API keys, configuring Gemini, and loading the base data. # If any of these critical steps fail, gemini_client.py itself calls exit().
try:
    import gemini_client
except SystemExit:
    print("CRITICAL: Failed to initialize the 'gemini_client' module. Check logs from gemini_client.py.")
    sys.exit(1) # Ensure this script also exits if analiser had a critical startup failure
except ImportError:
    print("CRITICAL: Could not import 'gemini_client' module. Ensure 'gemini_client.py' is in the same directory or Python path.")
    sys.exit(1)


# --- Main execution for Gradio App ---
if __name__ == "__main__":
    # Perform checks to ensure analiser module was initialized correctly
    if not gemini_client.GEMINI_API_KEY or gemini_client.GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE":
        print("CRITICAL: GEMINI_API_KEY not configured. 'gemini_client.py' should have caught this.")
        sys.exit(1)
    if not gemini_client.model:
        print("CRITICAL: Gemini model not initialized in 'gemini_client' module.")
        sys.exit(1)
    if gemini_client.df_transformed is None:
        print("CRITICAL: Transformed data not loaded by 'gemini_client' module.")
        sys.exit(1)

    # Load and prepare data using analiser's function
    # This populates global variables within the analiser module
    data_prep_error = gemini_client.load_and_prepare_data_for_analysis()
    if data_prep_error:
        print(f"CRITICAL Error during data preparation: {data_prep_error}")
        print("Cannot start Gradio app. Exiting.")
        sys.exit(1)

    if gemini_client.highest_fiscal_week_for_analysis is None:
        print(f"CRITICAL Error: Fiscal week for analysis not determined after data preparation.")
        print("Cannot start Gradio app. Exiting.")
        sys.exit(1)

    # Get initial analysis summary using analiser's function
    initial_summary_text = gemini_client.get_initial_gemini_analysis()
    print(f"DEBUG: Content of initial_summary_text before displaying:\n---\n{initial_summary_text}\n---") # For console diagnosis
    # You might want to add more robust error checking for initial_summary_text here
    if not initial_summary_text or not isinstance(initial_summary_text, str):
        initial_summary_text = "The analysis summary could not be generated or is empty. Please check the console logs for more details from 'gemini_client.py'."


    with gr.Blocks(theme=gr.themes.Soft(), title="Sales Performance Analysis") as app:
        gr.Markdown(f"# Sales Performance Analysis: {gemini_client.current_time_period_for_analysis} - Week {int(gemini_client.highest_fiscal_week_for_analysis)}")

        with gr.Tab("Analysis Summary"):
            summary_output = gr.Markdown(value=initial_summary_text)

        with gr.Tab("Ask Questions about this Week's Data"):
            gr.Markdown(f"Ask a question about the data for **{gemini_client.current_time_period_for_analysis} - Week {int(gemini_client.highest_fiscal_week_for_analysis)}**.")
            question_input = gr.Textbox(label="Your Question:", placeholder="e.g., Which Cohort has the lowest YearOnYearGrowth for SUM of SALES?")
            answer_output = gr.Markdown(label="Gemini's Answer:")
            ask_button = gr.Button("Ask Gemini", variant="primary")

            ask_button.click(fn=gemini_client.answer_user_question, inputs=[question_input], outputs=[answer_output])
        
        gr.Markdown("--- \n *Powered by Gemini. Analysis is based on the single fiscal week shown.*")

    print("Launching Gradio app locally from gradio_interface.py...")
    app.launch(server_name="127.0.0.1", share=False)