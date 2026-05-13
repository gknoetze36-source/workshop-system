from database import initialize_database


if __name__ == "__main__":
    state = initialize_database()
    print(f"Database updated safely: {state['backend']}")
