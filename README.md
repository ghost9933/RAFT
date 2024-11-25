# Raft Consensus Implementation

## Setup
1. **Clone the repository:**
    ```bash
    git clone https://github.com/ghost9933/RAFT.git
    cd RAFT
    ```

2. **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    Linux/MAC:  source venv/bin/activate  #
    Windows:    venv\Scripts\activate
    ```

3. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Start the Raft cluster:**
    ```bash
    docker-compose up -d
    ```
5. **check logs of the Nodes**

    ```bash
     docker-compose up -d
     <!-- To start all the required nodes  -->
     docker ps -a --format "{{.Names}}" 
     <!-- Llist the names of the current runnign contrainers  -->
     docker-compose logs node2 
     <!-- enter any one of the node names to check thero logs -->
    
    ```
6. **cManualy running the client**

    ```bash
     python client.py localhost 50052 "set y=10"
    
    ```


## Running Tests
1. **Ensure the virtual environment is activated.**

2. **Execute the test script:**
    ```bash
    python test.py
    ```

## Notes
- To stop the cluster, run `docker-compose down`.
- Ensure no persistent volumes are used to allow nodes to start fresh on restart.
