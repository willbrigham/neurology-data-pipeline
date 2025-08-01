# Set your Python interpreter
PYTHON := python3
VENV_DIR := venv

.PHONY: setup run freeze clean

# 1. Create virtual environment and install dependencies
setup:
	$(PYTHON) -m venv $(VENV_DIR)
	. $(VENV_DIR)/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# 2. Run your main Spark exploration script (customize this as needed)
run:
	. $(VENV_DIR)/bin/activate && python scripts/explore_data_spark.py

# 3. Freeze current env dependencies into requirements.txt
freeze:
	. $(VENV_DIR)/bin/activate && pip freeze > requirements.txt

# 4. Remove venv to start fresh
clean:
	rm -rf $(VENV_DIR)