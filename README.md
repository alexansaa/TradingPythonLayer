<a name="readme-top"></a>

# 📗 Table of Contents

- [📖 About the Project](#about-project)
  - [🛠 Built With](#built-with)
    - [Tech Stack](#tech-stack)
    - [Key Features](#key-features)
- [💻 Getting Started](#getting-started)
  - [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Install](#install)
  - [Usage](#usage)
  - [Run tests](#run-tests)
  - [Deployment](#deployment)
- [👥 Authors](#authors)
- [🔭 Future Features](#future-features)
- [🤝 Contributing](#contributing)
- [⭐️ Show your support](#support)
- [🙏 Acknowledgements](#acknowledgements)
- [📝 License](#license)

# 📖 [Trading Python Layer] <a name="about-project"></a>

**[Trading Python Layer]**
This project represents the Python layer of the Trading-App Stack, a containerized multi-service platform for financial data analysis and trading simulation. It serves as the data ingestion and analytics service, responsible for fetching, processing, and exposing market data to the other components of the system.

The Python layer is Dockerized and deployed automatically through a DevOps CI/CD pipeline, ensuring consistent builds, environment isolation, and reliable updates to the production and certification environments.

This layer bridges the external financial data providers and the application’s internal analytics engine, serving as the data backbone of the entire trading stack.

## 🛠 Built With <a name="built-with"></a>

### Tech Stack <a name="tech-stack"></a>

<details>
  <summary>Core Technologies</summary>
  <ul>
    <li><a href="https://www.python.org/">Python</a></li>
    <li><a href="https://fastapi.tiangolo.com/">FastApi</a></li>
    <li><a href="https://www.docker.com/">Docker</a></li>
    <li><a href="https://azure.microsoft.com/es-es/products/devops">DevOps</a></li>
  </ul>
</details>

### Key Features <a name="key-features"></a>

- ⚙️ **Market Data Integration** Connects to external APIs (e.g., Tiingo) to retrieve end-of-day (EOD) or intraday price data for configured stock symbols.
- 🗄️ **Database Sync** Performs incremental upserts into the SQL Server `market.PriceBar` table, ensuring that the latest data is stored without duplication.
- 🌐 **API Service** Provides REST endpoints (built with FastAPI) for retrieving processed data (e.g., /prices/latest, /healthz) and enabling interoperability with other layers such as the Java backend or frontend dashboard.
- ⏱️ **Background Scheduler** Uses APScheduler to automate periodic data updates, respecting API rate limits and resuming from the last known date.
- 🐳 **Containerized Deployment** Runs as a Dockerized service, designed to integrate seamlessly into the multi-container environment (trading-core network).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 💻 Getting Started <a name="getting-started"></a>

To get a local copy up and running, follow these steps.

### Prerequisites

- Python 3.10+ (recommended for compatibility with FastAPI and APScheduler)
- Docker & Docker Compose (for containerized deployment)
- Git (to clone the repository and manage source control)
- SQL Server instance running within the same Docker network (trading-core)
- Azure DevOps Agent (if deploying via CI/CD pipeline)

### Setup

Clone this repository to your desired folder:

```sh
  git clone https://github.com/alexansaa/TradingPythonLayer.git
  cd TradingPythonLayer
```
Ensure the shared network exists. The Python container must communicate with the SQL container through the shared Docker network:

```sh
  docker network create trading-core || true
```
Build and Start the Container. Use Docker Compose (or the CI/CD pipeline) to build and start the service:

```sh
  docker compose up --build -d
```
Verify the Deployment. Once running, verify that the API and scheduler are active:

```sh
  curl http://localhost:18080/healthz
```
Expected response:

```json
  {"status": "ok"}
```
Monitor Logs (Optional). To view the container logs in real time:

```sh
  docker compose logs -f python-layer
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- AUTHORS -->

## 👥 Authors <a name="authors"></a>

👤 **Alexander**

- GitHub: [https://github.com/alexansaa](https://github.com/alexansaa)
- LinkedIn: [https://www.linkedin.com/in/alexander-saavedra-garcia/](https://www.linkedin.com/in/alexander-saavedra-garcia/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- FUTURE FEATURES -->

## 🔭 Future Features <a name="future-features"></a>

- [ ] 🧠 **[AI/ML model and trading signals forecasting]**
- [ ] 🌎 **[Multi-market data incorporation]**
- [ ] ⚡ **[Real-time data streaming]**
- [ ] 💹 **[Trading Simulation Engine]**
- [ ] 📊 **[Performance Analytics Dashboard]**

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->

## 🤝 Contributing <a name="contributing"></a>

Contributions, issues, and feature requests are welcome!

Feel free to check the [issues page](https://github.com/alexansaa/TradingPythonLayer/issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## ⭐️ Show your support <a name="support"></a>

If you like this project, please give it a star on GitHub

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## 🙏 Acknowledgments <a name="acknowledgements"></a>

I’d like to thank my wife for her patience and unwavering support during my darkest and most isolated days, when completing systems like these demanded every bit of my time, focus, and perseverance

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->

## 📝 License <a name="license"></a>

This project is licensed under the [GNU](./LICENSE.md) General Public License.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
