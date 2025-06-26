#!/usr/bin/env python3
import os
import re
import time
import random
import asyncio
import aiohttp
import tldextract
import questionary
from typing import List, Dict, Optional, Tuple, Set, Any
from datetime import datetime
from urllib.parse import quote_plus, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import json
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.console import Console
from rich.panel import Panel
from rich.logging import RichHandler
from abc import ABC, abstractmethod
import requests
import sys
from dataclasses import dataclass
import backoff
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=Console(force_terminal=True))]
)
logger = logging.getLogger(__name__)

# Initialize console
console = Console(
    force_terminal=True,
    color_system="auto",
    width=120
)

# ========== CONFIGURATION ========== #
@dataclass
class Config:
    """Configuration settings with environment variable support"""
    MAX_RESULTS: int = 500
    DELAY_RANGE: Tuple[float, float] = (0.5, 2.5)
    OUTPUT_FILE: str = f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    AI_ENRICHMENT: bool = True
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    AI_EXPANSION_DEPTH: int = 3
    PROXY_ENABLED: bool = True
    PROXY_LIST: str = "proxies.txt"
    HEADLESS: bool = True
    CAPTCHA_SOLVER: str = None
    VALIDATE_EMAILS: bool = True
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables with error handling"""
        try:
            cls.MAX_RESULTS = int(os.getenv('MAX_RESULTS', '500'))
            cls.AI_ENRICHMENT = os.getenv('AI_ENRICHMENT', 'true').lower() == 'true'
            cls.PROXY_ENABLED = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
            cls.HEADLESS = os.getenv('HEADLESS', 'true').lower() == 'true'
            
            # Parse DELAY_RANGE from string like "1.0,2.5"
            delay_str = os.getenv('DELAY_RANGE', '0.5,2.5')
            cls.DELAY_RANGE = tuple(map(float, delay_str.split(',')))
            
            # Handle proxy list loading
            if cls.PROXY_ENABLED:
                proxy_source = os.getenv('PROXY_LIST', 'proxies.txt')
                if os.path.exists(proxy_source):
                    with open(proxy_source) as f:
                        cls.PROXY_LIST = [p.strip() for p in f if p.strip()]
                else:
                    cls.PROXY_LIST = [p.strip() for p in proxy_source.split(',') if p.strip()]
            
            cls.validate()
            return cls
            
        except ValueError as e:
            cls.validate()
            raise ValueError(f"Invalid environment variable format: {str(e)}")
    
    @classmethod
    def validate(cls):
        """Validate configuration settings"""
        # Validate DELAY_RANGE
        if not isinstance(cls.DELAY_RANGE, tuple) or len(cls.DELAY_RANGE) != 2:
            raise ValueError("DELAY_RANGE must be a tuple of (min, max)")
        if cls.DELAY_RANGE[0] < 0.3 or cls.DELAY_RANGE[1] < cls.DELAY_RANGE[0]:
            raise ValueError("Invalid delay range values")
        
        # Validate other settings
        if not isinstance(cls.MAX_RESULTS, int) or cls.MAX_RESULTS <= 0:
            raise ValueError("MAX_RESULTS must be a positive integer")
        if cls.MAX_RESULTS > 1000:
            raise ValueError("Result limit too high")
        
        if not isinstance(cls.AI_EXPANSION_DEPTH, int) or cls.AI_EXPANSION_DEPTH <= 0:
            raise ValueError("Expansion depth must be positive")
        
        # Validate proxy settings
        if cls.PROXY_ENABLED and (not hasattr(cls, 'PROXY_LIST') or not cls.PROXY_LIST):
            raise ValueError("Proxy enabled but no proxies provided")
  
# ========== PROGRESS TRACKER ========== #
class ProgressTracker:
    """Enhanced progress tracking with task management"""
    def __init__(self):
        self.progress = Progress(
            SpinnerColumn(style="bold cyan"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(complete_style="blue", pulse_style="white"),
            TimeElapsedColumn(),
            console=console
        )
        self.tasks = {}
        self.completed = set()

    def add_task(self, name: str, total: int = 100):
        """Add a new tracking task"""
        if name in self.tasks:
            return self.tasks[name]
            
        task_id = self.progress.add_task(f"[cyan]{name}", total=total)
        self.tasks[name] = task_id
        return task_id

    def update(self, name: str, advance: int = 1):
        """Update task progress"""
        if name in self.tasks and name not in self.completed:
            self.progress.update(self.tasks[name], advance=advance)
            if self.progress.tasks[self.tasks[name]].completed >= self.progress.tasks[self.tasks[name]].total:
                self.completed.add(name)

    def complete_task(self, name: str):
        """Mark task as fully completed"""
        if name in self.tasks:
            self.progress.update(self.tasks[name], completed=self.progress.tasks[self.tasks[name]].total)
            self.completed.add(name)

    def __enter__(self):
        self.progress.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()

# ========== PROXY MANAGER ========== #
class ProxyManager:
    """Rotating proxy management system"""
    def __init__(self, proxy_list: List[str] = None):
        self.proxies = proxy_list or []
        self.current_index = 0
        self.failed_proxies = set()
        
    def get_next_proxy(self) -> Optional[str]:
        """Get next available proxy in rotation"""
        if not self.proxies:
            return None
            
        if len(self.failed_proxies) >= len(self.proxies):
            return None  # All proxies failed
            
        while True:
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            if proxy not in self.failed_proxies:
                return proxy
                
    def mark_failed(self, proxy: str):
        """Mark a proxy as failed"""
        if proxy in self.proxies:
            self.failed_proxies.add(proxy)
            
    def get_proxy_dict(self, proxy: str) -> Dict:
        """Format proxy for aiohttp/requests"""
        if not proxy:
            return {}
            
        if proxy.startswith('http'):
            return {'http': proxy, 'https': proxy}
        else:
            return {'http': f'http://{proxy}', 'https': f'https://{proxy}'}

# ========== AI QUERY OPTIMIZER ========== #
class AIQueryOptimizer:
    """
    Enhanced query optimization with more comprehensive expansions
    """
    ROLE_HIERARCHY = {
        "executive": [
            "ceo", "owner", "coo", "cfo", "cto", "cio", "founder", 
            "partner", "president", "vp", "director", "managing director",
            "board member", "chairman", "principal"
        ],
        "manager": [
            "manager", "senior manager", "head", "lead", "supervisor",
            "team lead", "department head", "group manager", "practice lead"
        ],
        "technical": [
            "engineer", "senior engineer", "developer", "senior developer",
            "architect", "data scientist", "analyst", "specialist", "consultant",
            "devops", "sre", "security engineer", "qa engineer", "systems administrator"
        ],
        "operations": [
            "operations manager", "hr manager", "finance manager", "office manager",
            "logistics coordinator", "supply chain manager", "facilities manager"
        ],
        "support": [
            "assistant", "coordinator", "administrator", "representative",
            "intern", "receptionist", "customer support", "helpdesk"
        ]
    }

    INDUSTRY_SYNONYMS = {
        "technology": [
    "software", "hardware", "IT", "cloud", "cybersecurity", "ai", "machine learning", 
    "blockchain", "fintech", "edtech", "healthtech", "saas", "iot", "gaming", "web3"
  ],
  "finance": [
    "banking", "investment", "asset management", "private equity", "venture capital", 
    "accounting", "insurance", "fintech", "cryptocurrency", "hedge funds", "stock trading"
  ],
  "healthcare": [
    "pharmaceuticals", "biotech", "medical devices", "hospitals", "telemedicine", 
    "health insurance", "clinics", "mental health", "healthtech"
  ],
  "construction": [
    "civil engineering", "architecture", "real estate development", "contracting", 
    "infrastructure", "urban planning", "construction management", "green building"
  ],
  "manufacturing": [
    "automotive", "aerospace", "electronics", "textiles", "industrial equipment", 
    "3D printing", "robotics", "supply chain", "chemicals"
  ],
  "retail": [
    "ecommerce", "fashion", "consumer goods", "luxury", "supermarkets", 
    "dropshipping", "marketplaces", "direct-to-consumer (DTC)"
  ],
  "energy": [
    "oil & gas", "renewables", "solar", "wind", "nuclear", "utilities", 
    "energy storage", "electric vehicles (EV)", "smart grid"
  ],
  "education": [
    "edtech", "e-learning", "higher education", "K-12", "vocational training", 
    "tutoring", "online courses", "corporate training"
  ],
  "entertainment": [
    "media", "film & TV", "music", "streaming", "gaming", "esports", 
    "publishing", "social media", "virtual reality (VR)"
  ],
  "transportation": [
    "logistics", "aviation", "shipping", "rail", "autonomous vehicles", 
    "ride-sharing", "public transit", "last-mile delivery"
        ],

    }

    @classmethod
    def expand_query(cls, job_title: str, industry: str, location: str) -> Tuple[List[str], List[str], List[str]]:
        """
        Generate multiple search variations with enhanced logic
        Returns: (titles, industries, locations)
        """
        titles = cls._expand_titles(job_title.lower())
        industries = cls._expand_industries(industry.lower())
        locations = cls._expand_location(location)
        
        # Limit expansion depth and ensure variety
        titles = sorted(list(set(titles)), key=lambda x: (len(x), x))[:Config.AI_EXPANSION_DEPTH]
        industries = sorted(list(set(industries)), key=lambda x: (len(x), x))[:Config.AI_EXPANSION_DEPTH]
        locations = sorted(list(set(locations)), key=len)[:Config.AI_EXPANSION_DEPTH]
        
        return titles, industries, locations
    
    @classmethod
    def _expand_location(cls, location: str) -> List[str]:
        """Generate related location terms"""
        expanded = {location}
        
        # Add common variations
        if ", " in location:
            city, country = location.split(", ", 1)
            expanded.add(city)
            expanded.add(country)
            expanded.add(f"{city} {country}")
        
        # Handle country/state variations
        if "usa" in location.lower() or "united states" in location.lower():
            expanded.update(["us", "united states", "america", "usa"])
        elif "uk" in location.lower() or "united kingdom" in location.lower():
            expanded.update(["united kingdom", "great britain", "england", "gb", "uk"])
        
        # Add common abbreviations
        if len(location.split()) > 1:
            expanded.add("".join(word[0] for word in location.split()))
        
        return list(expanded)
        
    @classmethod
    def _expand_titles(cls, title: str) -> List[str]:
        """Generate related job titles with more variations"""
        expanded = {title}
        
        # Add common variations
        if " " in title:
            expanded.add(title.replace(" ", "-"))
            expanded.add(title.replace(" ", ""))
        
        # Add hierarchical variations
        for role, variants in cls.ROLE_HIERARCHY.items():
            if any(v in title for v in variants):
                expanded.update(variants)
                expanded.update(f"senior {v}" for v in variants)
                expanded.update(f"chief {v}" for v in variants if v not in ["ceo", "cto", "cfo", "cio"])
        
        # Add common prefixes/suffixes
        prefixes = ["lead", "senior", "junior", "chief", "principal", "head of"]
        suffixes = ["manager", "director", "specialist", "engineer"]
        
        base_title = title.split()[-1]
        for prefix in prefixes:
            expanded.add(f"{prefix} {base_title}")
        for suffix in suffixes:
            expanded.add(f"{base_title} {suffix}")
        
        return list(expanded)
        
    @classmethod
    def _expand_industries(cls, industry: str) -> List[str]:
        """Generate related industry terms"""
        expanded = {industry.lower()}
        
        # Add common variations
        if " " in industry:
            expanded.add(industry.replace(" ", "-"))
            expanded.add(industry.replace(" ", ""))
        
        # Add synonyms from INDUSTRY_SYNONYMS
        for main_industry, variants in cls.INDUSTRY_SYNONYMS.items():
            if any(v in industry.lower() for v in variants):
                expanded.update(variants)
        
        return list(expanded)[:Config.AI_EXPANSION_DEPTH]

# ========== API MANAGER ========== #
class APIVault:
    SERVICES = {
        "ScrapingBee": {
            "pattern": r"^[a-zA-Z0-9]{40,}$",
            "help": "Get from https://www.scrapingbee.com (free tier available)",
            "required": False,
            "purpose": "Bypasses website blocks for more reliable scraping"
        },
        "Hunter.io": {
            "pattern": r"^[a-f0-9]{32}$",
            "help": "Register at https://hunter.io (free tier available)",
            "required": False, 
            "purpose": "Finds professional email addresses"
        },
        "Clearbit": {
            "pattern": r"^sk_[a-zA-Z0-9]{32}$",
            "help": "Optional - Get from https://clearbit.com",
            "required": False,
            "purpose": "Enriches company information"
        }
    }

    @classmethod
    async def configure(cls):
        """Interactive API key setup with clear explanations"""
        console.print(Panel.fit(
            "[bold]API Key Configuration[/]",
            subtitle="Leave blank to skip optional APIs"
        ))
        
        keys = {}
        
        for service, config in cls.SERVICES.items():
            console.print(Panel.fit(
                f"[bold]{service}[/]\n"
                f"[dim]Purpose:[/] {config['purpose']}\n"
                f"[dim]Where to get it:[/] {config['help']}",
                border_style="blue"
            ))
            
            while True:
                key = await questionary.text(
                    f"Enter your {service} API key (or press Enter to skip):",
                    validate=lambda x: True if not x or re.match(config["pattern"], x) 
                    else f"Invalid format! {config['help']}"
                ).unsafe_ask_async()
                
                if not key:
                    if config["required"]:
                        console.print(f"[yellow]Warning: {service} is recommended for best results[/]")
                    else:
                        console.print(f"[dim]Skipping {service}[/]")
                    break
                    
                if re.match(config["pattern"], key):
                    keys[service] = key
                    console.print(f"[green]✓ {service} key accepted[/]")
                    break
                    
                console.print(f"[red]Invalid {service} key format![/]")
                
        return keys

# ========== LANGUAGE SUPPORT ========== #
class PolyglotScraper:
    LANGUAGE_MAP = {
  "English": {
    "code": "en",
    "titles": {
      "CEO": "CEO",
      "Manager": "Manager",
      "Founder": "Founder",
      "Engineer": "Engineer"
    },
    "google_domain": "google.com",
    "linkedin_domain": "www.linkedin.com"
  },
  "Japanese": {
    "code": "ja",
    "titles": {
      "CEO": "代表取締役社長",
      "Manager": "マネージャー",
      "Founder": "創業者",
      "Engineer": "エンジニア"
    },
    "google_domain": "google.co.jp",
    "linkedin_domain": "jp.linkedin.com"
  },
  "Spanish": {
    "code": "es",
    "titles": {
      "CEO": "Director Ejecutivo",
      "Manager": "Gerente",
      "Founder": "Fundador",
      "Engineer": "Ingeniero"
    },
    "google_domain": "google.es",
    "linkedin_domain": "es.linkedin.com"
  },
  "German": {
    "code": "de",
    "titles": {
      "CEO": "Geschäftsführer",
      "Manager": "Manager",
      "Founder": "Gründer",
      "Engineer": "Ingenieur"
    },
    "google_domain": "google.de",
    "linkedin_domain": "de.linkedin.com"
  },
  "French": {
    "code": "fr",
    "titles": {
      "CEO": "PDG",
      "Manager": "Manager",
      "Founder": "Fondateur",
      "Engineer": "Ingénieur"
    },
    "google_domain": "google.fr",
    "linkedin_domain": "fr.linkedin.com"
  },
  "Chinese (Simplified)": {
    "code": "zh",
    "titles": {
      "CEO": "首席执行官",
      "Manager": "经理",
      "Founder": "创始人",
      "Engineer": "工程师"
    },
    "google_domain": "google.cn",
    "linkedin_domain": "cn.linkedin.com"
  },
  "Portuguese": {
    "code": "pt",
    "titles": {
      "CEO": "Diretor Executivo",
      "Manager": "Gerente",
      "Founder": "Fundador",
      "Engineer": "Engenheiro"
    },
    "google_domain": "google.com.br",
    "linkedin_domain": "br.linkedin.com"
  },
  "Russian": {
    "code": "ru",
    "titles": {
      "CEO": "Генеральный директор",
      "Manager": "Менеджер",
      "Founder": "Основатель",
      "Engineer": "Инженер"
    },
    "google_domain": "google.ru",
    "linkedin_domain": "ru.linkedin.com"
  },
  "Arabic": {
    "code": "ar",
    "titles": {
      "CEO": "الرئيس التنفيذي",
      "Manager": "مدير",
      "Founder": "مؤسس",
      "Engineer": "مهندس"
    },
    "google_domain": "google.com.sa",
    "linkedin_domain": "sa.linkedin.com"
  },
  "Hindi": {
    "code": "hi",
    "titles": {
      "CEO": "मुख्य कार्यकारी अधिकारी",
      "Manager": "प्रबंधक",
      "Founder": "संस्थापक",
      "Engineer": "इंजीनियर"
    },
    "google_domain": "google.co.in",
    "linkedin_domain": "in.linkedin.com"
  }
}

    @classmethod
    async def select_language(cls):
        """Interactive language selection"""
        lang = await questionary.select(
            "Select search language:",
            choices=list(cls.LANGUAGE_MAP.keys())
        ).unsafe_ask_async()
        return cls.LANGUAGE_MAP.get(lang, cls.LANGUAGE_MAP["English"])

    @classmethod
    def translate_title(cls, title: str, lang_config: dict) -> str:
        """Localize job titles"""
        return lang_config["titles"].get(title, title)  
        
# ========== GEO TARGETING ========== #
class GeoExplorer:
    CONTINENTS = {
  "Africa": [
    "DZ", "AO", "BJ", "BW", "BF", "BI", "CV", "CM", "CF", "TD", "KM", "CG", "CD", "CI", "DJ", "EG", "GQ", "ER", "SZ", "ET", 
    "GA", "GM", "GH", "GN", "GW", "KE", "LS", "LR", "LY", "MG", "MW", "ML", "MR", "MU", "MA", "MZ", "NA", "NE", "NG", "RW", 
    "ST", "SN", "SC", "SL", "SO", "ZA", "SS", "SD", "TZ", "TG", "TN", "UG", "ZM", "ZW"
  ],
  "Asia": [
    "AF", "AM", "AZ", "BH", "BD", "BT", "BN", "KH", "CN", "CY", "GE", "IN", "ID", "IR", "IQ", "IL", "JP", "JO", "KZ", "KW", 
    "KG", "LA", "LB", "MY", "MV", "MN", "MM", "NP", "KP", "OM", "PK", "PH", "QA", "RU", "SA", "SG", "KR", "LK", "SY", "TW", 
    "TJ", "TH", "TL", "TR", "TM", "AE", "UZ", "VN", "YE", "PS"
  ],
  "Europe": [
    "AL", "AD", "AT", "BY", "BE", "BA", "BG", "HR", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT", "XK", 
    "LV", "LI", "LT", "LU", "MT", "MD", "MC", "ME", "NL", "MK", "NO", "PL", "PT", "RO", "SM", "RS", "SK", "SI", "ES", "SE", 
    "CH", "UA", "GB", "VA"
  ],
  "North America": [
    "AG", "BS", "BB", "BZ", "CA", "CR", "CU", "DM", "DO", "SV", "GD", "GT", "HT", "HN", "JM", "MX", "NI", "PA", "KN", "LC", 
    "VC", "TT", "US"
  ],
  "Oceania": [
    "AU", "FJ", "KI", "MH", "FM", "NR", "NZ", "PW", "PG", "WS", "SB", "TO", "TV", "VU", "AS", "CK", "PF", "GU", "NC", "NU", 
    "MP", "PN", "TK", "WF"
  ],
  "South America": [
    "AR", "BO", "BR", "CL", "CO", "EC", "GY", "PY", "PE", "SR", "UY", "VE"
  ],
  "Antarctica": ["AQ"]
}
    @classmethod
    async def select_region(cls):
        """Select target region with validation"""
        while True:
            region = await questionary.select(
                "Target region:",
                choices=list(cls.CONTINENTS.keys()) + ["Global"]
            ).unsafe_ask_async()
            
            if region == "Global":
                return []
                
            if region in cls.CONTINENTS:
                return cls.CONTINENTS[region]
            
            console.print("[yellow]Invalid region selected, please try again[/]")
            
# ========== BASE SCRAPER CLASS ========== #
class Scraper(ABC):
    def __init__(self, progress: ProgressTracker):
        self.progress = progress
        self.ua = UserAgent()

    @abstractmethod
    async def scrape(self, query: Dict):
        """Scrape data based on the provided query
        
        Args:
            query: Dictionary containing search parameters
            
        Returns:
            List of scraped results
        """
        pass  # Just the method signature, implementation in subclasses

    @abstractmethod
    async def _init_browser(self):
        """Initialize browser instance (to be implemented by subclasses)"""
        pass          
        
# ========== LINKEDIN SCRAPER ========== #
class LinkedInScraper(Scraper):
    def __init__(self, progress: ProgressTracker, api_key: str = None, 
                 lang_config: dict = None, proxy_manager: ProxyManager = None):
        """
        Enhanced LinkedIn scraper with:
        - Better anti-detection measures
        - Improved error handling
        - Realistic human-like behavior
        """
        super().__init__(progress)
        self.api_key = api_key
        self.lang_config = lang_config or {"code": "en", "titles": {}}
        self.proxy_manager = proxy_manager
        self.driver = None
        self.session = None
        self.retry_count = 0
        self.RATE_LIMIT = 5  # requests per minute
        self.last_request_time = 0
        self._login_attempted = False

    async def scrape(self, query: Dict) -> List[Dict]:
        """Main scraping method with comprehensive error handling"""
        self.progress.add_task(f"LinkedIn ({query['location']})", total=100)
        results = []
        
        try:
            await self._enforce_rate_limit()
            
            if not self.api_key and not self.driver:
                await self._init_browser()
                if not self._login_attempted:
                    await self._linkedin_login()
            
            url = self._build_linkedin_url(
                query['job_title'],
                query['location'],
                self.lang_config
            )
            
            if await self._check_for_blocking():
                raise Exception("LinkedIn blocking detected")
                
            if self.api_key:
                results = await self._scrape_via_api(url)
            else:
                results = await self._scrape_via_selenium(url)
            
            return results
        
        except Exception as e:
            logger.error(f"LinkedIn Error: {str(e)}")
            if self.retry_count < Config.MAX_RETRIES:
                self.retry_count += 1
                await asyncio.sleep(5)
                return await self.scrape(query)
            return []
        finally:
            self.progress.complete_task(f"LinkedIn ({query['location']})")

    async def _init_browser(self) -> None:
        """Initialize browser with advanced anti-detection measures"""
        chrome_options = Options()
        
        # Anti-bot detection settings
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-blink-features")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-extensions")
        
        # Performance and stability
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        
        # Realistic browser settings
        chrome_options.add_argument("--window-size=1280,720")
        chrome_options.add_argument(f"user-agent={self._get_realistic_user_agent()}")
        
        if Config.HEADLESS:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1920,1080")
        
        # Persistent session
        chrome_options.add_argument("--profile-directory=Default")
        chrome_options.add_argument("--user-data-dir=./chrome_profile")
        
        # Proxy configuration
        if Config.PROXY_ENABLED and self.proxy_manager:
            proxy = await self.proxy_manager.get_next_proxy()
            if proxy:
                chrome_options.add_argument(f"--proxy-server={proxy}")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Advanced stealth configuration
            stealth(
                self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            
            # Additional anti-detection tricks
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": self._get_realistic_user_agent()
            })
            
        except Exception as e:
            logger.error(f"Browser initialization failed: {str(e)}")
            raise

    async def _linkedin_login(self) -> None:
        """Perform LinkedIn login with realistic behavior"""
        if not self.driver or self._login_attempted:
            return
            
        try:
            await self._human_like_delay()
            self.driver.get("https://www.linkedin.com/login")
            
            # Wait for page to load naturally
            await self._human_like_delay(1.0, 2.0)
            
            # Fill credentials with human-like timing
            username = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            await self._type_like_human(username, "your_email@example.com")
            
            password = self.driver.find_element(By.ID, "password")
            await self._type_like_human(password, "your_password")
            
            # Random mouse movement before clicking
            await self._move_mouse_to_element(username)
            await self._human_like_delay(0.5, 1.5)
            
            submit = self.driver.find_element(By.CSS_SELECTOR, "button[type=submit]")
            submit.click()
            
            # Wait for login to complete
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "global-nav"))
            )
            self._login_attempted = True
            
            # Post-login random activity
            await self._human_like_delay(2.0, 4.0)
            
        except Exception as e:
            logger.warning(f"Login failed: {str(e)}")
            self._login_attempted = False

    async def _scrape_via_selenium(self, url: str) -> List[Dict]:
        """Scrape using Selenium with human-like interactions"""
        if not self.driver:
            raise Exception("Browser not initialized")
            
        try:
            # Simulate human navigation
            await self._human_like_delay()
            self.driver.get(url)
            
            # Random scrolling
            await self._simulate_scrolling()
            
            # Wait for results naturally
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".search-results-container"))
            )
            
            # Check for blocking
            if await self._check_for_blocking():
                raise Exception("Blocking detected during scraping")
                
            # Additional random delay
            await self._human_like_delay(1.0, 3.0)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            return self._parse_html(soup)
            
        except Exception as e:
            logger.error(f"Selenium scraping failed: {str(e)}")
            raise

    def _parse_html(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse LinkedIn search results with robust error handling"""
        results = []
        container = soup.select_one(".search-results-container")
        
        if not container:
            logger.error("No results container found - may be blocked")
            return results
        
        for result in container.select(".entity-result"):
            try:
                # Safely extract all elements
                name_elem = result.select_one(".entity-result__title-text a")
                title_elem = result.select_one(".entity-result__primary-subtitle")
                location_elem = result.select_one(".entity-result__secondary-subtitle")
                
                if not all([name_elem, title_elem, location_elem]):
                    continue
                    
                # Clean and validate URL
                profile_url = name_elem["href"].split('?')[0]
                if not profile_url.startswith('http'):
                    profile_url = f"https://www.linkedin.com{profile_url}"
                
                # Build result dict
                results.append({
                    "name": name_elem.text.strip(),
                    "url": profile_url,
                    "title": title_elem.text.strip(),
                    "location": location_elem.text.strip(),
                    "source": "LinkedIn",
                    "timestamp": datetime.now().isoformat(),
                    "quality_score": self._calculate_profile_quality(
                        name_elem.text.strip(),
                        title_elem.text.strip()
                    )
                })
                
            except Exception as e:
                logger.debug(f"Skipping malformed result: {str(e)}")
                continue
                
        return results

    async def _scrape_via_api(self, url: str) -> List[Dict]:
        """Scrape using API with comprehensive error handling"""
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        params = {
            "api_key": self.api_key,
            "url": url,
            "render_js": "true",
            "wait_for": ".search-results-container",
            "wait": "5000",
            "premium_proxy": "true" if Config.PROXY_ENABLED else "false"
        }
        
        try:
            async with self.session.get(
                "https://app.scrapingbee.com/api/v1",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"ScrapingBee error: {error}")
                    if "proxy" in params:
                        await self.proxy_manager.mark_failed(params["proxy"])
                    raise Exception(f"API Error: {error}")
                
                html = await resp.text()
                return self._parse_html(BeautifulSoup(html, 'html.parser'))
                
        except Exception as e:
            logger.error(f"API Request failed: {str(e)}")
            raise

    async def cleanup(self) -> None:
        """Comprehensive resource cleanup"""
        try:
            if self.driver:
                self.driver.quit()
            if self.session and not self.session.closed:
                await self.session.close()
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")

    # ========== HELPER METHODS ========== #
    def _build_linkedin_url(self, job_title: str, location: str, lang_config: dict) -> str:
        """Construct optimized LinkedIn search URL"""
        base_url = f"https://{lang_config.get('linkedin_domain', 'www.linkedin.com')}/search/results/people/"
        params = {
            "keywords": f"{job_title} {location}",
            "origin": "GLOBAL_SEARCH_HEADER",
            "sid": "2*B"  # Magic string that helps avoid detection
        }
        return f"{base_url}?{'&'.join(f'{k}={quote_plus(v)}' for k, v in params.items())}"

    async def _enforce_rate_limit(self) -> None:
        """Smart rate limiting with exponential backoff"""
        now = time.time()
        elapsed = now - self.last_request_time
        min_delay = 60 / self.RATE_LIMIT
        
        if elapsed < min_delay:
            backoff = min_delay - elapsed + random.uniform(0.5, 1.5)
            await asyncio.sleep(backoff)
            
        self.last_request_time = time.time()

    async def _check_for_blocking(self) -> bool:
        """Comprehensive blocking detection"""
        if not self.driver:
            return False
        
        blocking_indicators = [
            "security check", "captcha", "verification",
            "too many requests", "restricted", "blocked"
        ]
        
        page_source = self.driver.page_source.lower()
        return any(indicator in page_source for indicator in blocking_indicators)

    async def _human_like_delay(self, min_sec: float = 1.5, max_sec: float = 4.5) -> None:
        """Randomized human-like delay with normal distribution"""
        mean = (min_sec + max_sec) / 2
        stddev = (max_sec - min_sec) / 4
        delay = max(min_sec, min(max_sec, random.normalvariate(mean, stddev)))
        await asyncio.sleep(delay)

    async def _type_like_human(self, element, text: str) -> None:
        """Simulate human typing behavior"""
        for char in text:
            element.send_keys(char)
            await asyncio.sleep(random.uniform(0.05, 0.3))
            
        # Random chance of backspacing
        if random.random() < 0.3:
            for _ in range(random.randint(1, 3)):
                element.send_keys(Keys.BACK_SPACE)
                await asyncio.sleep(random.uniform(0.1, 0.5))
                element.send_keys(text[-1])
                await asyncio.sleep(random.uniform(0.1, 0.3))

    async def _move_mouse_to_element(self, element) -> None:
        """Simulate human-like mouse movement"""
        if not self.driver:
            return
            
        action = ActionChains(self.driver)
        try:
            # Move to element with slight offset
            action.move_to_element_with_offset(
                element, 
                random.randint(-5, 5),
                random.randint(-5, 5)
            ).perform()
            await asyncio.sleep(random.uniform(0.2, 0.8))
        except:
            pass

    async def _simulate_scrolling(self) -> None:
        """Simulate natural scrolling behavior"""
        if not self.driver:
            return
            
        try:
            scroll_pause = random.uniform(0.5, 1.5)
            scroll_amount = random.randint(200, 800)
            
            for _ in range(random.randint(2, 5)):
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                await asyncio.sleep(scroll_pause)
                
            # Sometimes scroll back up a bit
            if random.random() < 0.4:
                self.driver.execute_script(f"window.scrollBy(0, -{scroll_amount//2})")
                await asyncio.sleep(scroll_pause)
                
        except Exception as e:
            logger.debug(f"Scrolling simulation failed: {str(e)}")

    def _calculate_profile_quality(self, name: str, title: str) -> float:
        """Calculate profile quality score (0-1)"""
        score = 0.5  # Base score
        
        # Name completeness
        name_parts = name.split()
        if len(name_parts) >= 2:
            score += 0.2
            
        # Title indicators
        title_lower = title.lower()
        if any(word in title_lower for word in ["manager", "director", "vp", "ceo"]):
            score += 0.2
        elif any(word in title_lower for word in ["founder", "owner", "principal"]):
            score += 0.15
            
        return min(1.0, max(0.0, score))  # Clamp between 0-1

    def _get_realistic_user_agent(self) -> str:
        """Get realistic user agent with device diversity"""
        user_agents = [
            # Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            
            # macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            
            # Linux
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            
            # Mobile
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 14; SM-S928U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36"
        ]
        return random.choice(user_agents)
# ========== ENHANCED LEAD ENRICHMENT ========== #
class LeadEnricher:
    """Enhanced lead enrichment with more data sources"""
    def __init__(self, progress: ProgressTracker, api_keys: dict):
        self.progress = progress
        self.api_keys = api_keys
        self.email_patterns = [
            "{first}.{last}@{domain}",
            "{f}{last}@{domain}",
            "{first}{last[0]}@{domain}",
            "{first}_{last}@{domain}",
            "{first[0]}{last}@{domain}",
            "{first}@{domain}",
        ]
        self._session = None  # For reusing aiohttp session

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()

    async def enrich(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Comprehensive lead enrichment"""
        if not Config.AI_ENRICHMENT:
            return lead

        task_name = f"Enriching {lead.get('name', 'lead')[:15]}..."
        self.progress.add_task(task_name, total=100)
        
        try:
            # Basic info extraction
            lead = self._extract_basic_info(lead)
            
            # Email discovery
            lead["emails"] = await self._find_emails(lead)
            
            # Company data
            if "company" not in lead or not lead["company"]:
                lead["company"] = await self._find_company(lead)
            
            # Social profiles
            lead = await self._find_social_profiles(lead)
            
            # Phone numbers
            if "phones" not in lead:
                lead["phones"] = self._extract_phones(lead)
            
            # Score lead quality
            lead["score"] = self._score_lead(lead)
            
            return lead
            
        except aiohttp.ClientError as e:
            logger.error(f"Network error during enrichment: {str(e)}")
            return lead
        except (ValueError, KeyError) as e:
            logger.error(f"Data processing error: {str(e)}")
            return lead
        except Exception as e:
            logger.error(f"Unexpected enrichment error: {str(e)}", exc_info=True)
            return lead
        finally:
            self.progress.complete_task(task_name)

    async def _find_emails(self, lead: Dict[str, Any]) -> List[str]:
        """Find emails using multiple methods"""
        emails = set()
        
        # Hunter.io API if available
        if "hunter.io" in self.api_keys:
            try:
                hunter_emails = await self._query_hunter_api(lead)
                emails.update(hunter_emails)
            except Exception as e:
                logger.warning(f"Hunter.io API failed: {str(e)}")

        # Email pattern guessing as fallback
        if lead.get("name") and lead.get("company"):
            guessed = self._guess_emails(lead)
            emails.update(email.lower() for email in guessed if self._validate_email_format(email))

    # Email verification if enabled
    async def _verify_emails(self, emails: Set[str]) -> List[str]:
        """Verify and filter emails"""
        if Config.VALIDATE_EMAILS and emails:
            verified = set()
            for email in emails:
                if await self._verify_email(email):
                    verified.add(email)
            return sorted(verified)
        return sorted(emails)

    def _guess_emails(self, lead: Dict[str, Any]) -> List[str]:
        """Generate probable email addresses using multiple patterns"""
        if not lead.get("name") or not lead.get("company"):
            return []

        try:
            # Handle names with middle names/initials
            name_parts = [p for p in lead["name"].lower().split() if p]
            if len(name_parts) < 2:
                return []
                
            first, last = name_parts[0], name_parts[-1]
            domain = self._extract_domain(lead["company"])
            if not domain:
                return []

            emails = []
            for pattern in self.email_patterns:
                try:
                    email = pattern.format(
                        first=first,
                        f=first[0],
                        last=last,
                        domain=domain
                    )
                    if self._validate_email_format(email):
                        emails.append(email)
                except (KeyError, IndexError, AttributeError):
                    continue
            
            return emails
        except Exception as e:
            logger.debug(f"Email guessing failed: {str(e)}", exc_info=True)
            return []

    def _validate_email_format(self, email: str) -> bool:
        """More robust email validation"""
        if not email or not isinstance(email, str):
            return False
        return bool(re.fullmatch(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", email))

    async def _query_hunter_api(self, lead: Dict[str, Any]) -> List[str]:
        """Query Hunter.io API for emails"""
        if not self._session:
            self._session = aiohttp.ClientSession()
            
        domain = self._extract_domain(lead.get("company", ""))
        if not domain:
            return []

        url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={self.api_keys['hunter.io']}"
        
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return [e['value'] for e in data.get('data', {}).get('emails', [])]
                logger.warning(f"Hunter API returned status {response.status}")
                return []
        except aiohttp.ClientError as e:
            logger.warning(f"Hunter.io API request failed: {str(e)}")
            return []

    def _extract_domain(self, company: str) -> str:
        """Extract domain from company name"""
        if not company or not isinstance(company, str):
            return ""
        # Basic cleanup - should be enhanced with actual domain parsing
        return f"{re.sub(r'[^a-zA-Z0-9]', '', company.lower())}.com"
            
        # Simple domain extraction - should be enhanced with actual domain lookup
        company_clean = re.sub(r"[^\w]", "", company.lower())
        return f"{company_clean}.com"

    # Other methods should be implemented or raise NotImplementedError
    def _extract_basic_info(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Basic info extraction not implemented")

    async def _find_company(self, lead: Dict[str, Any]) -> str:
        raise NotImplementedError("Company lookup not implemented")

    async def _find_social_profiles(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Social profile lookup not implemented")

    def _extract_phones(self, lead: Dict[str, Any]) -> List[str]:
        raise NotImplementedError("Phone extraction not implemented")

    def _score_lead(self, lead: Dict[str, Any]) -> float:
        raise NotImplementedError("Lead scoring not implemented")

    async def _verify_email(self, email: str) -> bool:
        raise NotImplementedError("Email verification not implemented")

    
# ========== BAIDU SCRAPER ========== #
class BaiduScraper(Scraper):
    def __init__(self, progress: ProgressTracker, lang_config: dict = None, 
                 proxy_manager: ProxyManager = None):
        super().__init__(progress)
        self.lang_config = lang_config or {"code": "zh", "titles": {}}
        self.proxy_manager = proxy_manager
        self.driver = None
        self.session = None
        self.retry_count = 0
        self._request_delay = Config.DELAY_RANGE  # Use configured delay range

    async def cleanup(self):
        """Clean up resources"""
        try:
            if self.driver:
                self.driver.quit()
            if self.session and not self.session.closed:
                await self.session.close()
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")

    @retry(stop=stop_after_attempt(Config.MAX_RETRIES), 
           wait=wait_exponential(multiplier=1, min=4, max=10))
    async def scrape(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape Baidu for LinkedIn profiles"""
        self.progress.add_task(f"Baidu ({query['location']})", total=100)
        results = []
        
        try:
            await self._random_delay()  # Add random delay between requests
            
            search_query = (
                f'site:linkedin.com/in/ intitle:"{quote_plus(query["job_title"])}" '
                f'"{quote_plus(query["industry"])}" "{quote_plus(query["location"])}"'
            )
            url = self._build_baidu_url(search_query)
            
            if not self.driver:
                await self._init_browser()
            
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, Config.REQUEST_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".result.c-container"))
                )
                
                # Additional check for CAPTCHA
                if self._check_for_captcha():
                    raise Exception("CAPTCHA detected")
                
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                results = self._parse_results(soup)
                return results
                
            except TimeoutException:
                logger.warning("Timeout waiting for search results")
                if self.retry_count < Config.MAX_RETRIES:
                    self.retry_count += 1
                    await self._init_browser()  # Reinitialize browser
                    return await self.scrape(query)
                raise
            
        except Exception as e:
            logger.error(f"Baidu Error: {str(e)}")
            if "CAPTCHA" in str(e):
                logger.warning("Consider using CAPTCHA solving service")
            return []
        finally:
            self.progress.complete_task(f"Baidu ({query['location']})")

    def _build_baidu_url(self, query: str) -> str:
        """Construct Baidu search URL with proper parameters"""
        return (
            f"https://www.baidu.com/s?"
            f"wd={query}&"
            f"rn={Config.MAX_RESULTS}&"
            f"ie=utf-8&"
            f"oe=utf-8&"
            f"cl=3&"
            f"tn=baidutop10"
        )

    def _parse_results(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse Baidu search results for LinkedIn profiles"""
        results = []
        for result in soup.select(".result.c-container"):
            try:
                title = result.select_one("h3 a").text.strip()
                url = result.select_one("h3 a")["href"]
                snippet = result.select_one(".c-abstract").text.strip() if result.select_one(".c-abstract") else ""
                
                # Only include LinkedIn profiles
                if "linkedin.com/in/" in url:
                    results.append({
                        "title": title,
                        "url": url,
                        "snippet": snippet,
                        "source": "Baidu"
                    })
            except Exception as e:
                logger.debug(f"Failed to parse result: {str(e)}")
                continue
        return results

    async def _init_browser(self):
        """Initialize browser with Baidu-specific settings"""
        chrome_options = Options()
        
        # Baidu-specific settings
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(f"user-agent={self._get_baidu_user_agent()}")
        chrome_options.add_argument("--lang=zh-CN")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        
        # Headless mode
        if Config.HEADLESS:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1920,1080")
        
        # Proxy configuration
        if Config.PROXY_ENABLED and self.proxy_manager:
            proxy = await self.proxy_manager.get_next_proxy()
            if proxy:
                chrome_options.add_argument(f"--proxy-server={proxy}")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Apply stealth settings
            stealth(
                self.driver,
                languages=["zh-CN", "zh"],
                vendor="Baidu",
                platform="Win32",
                fix_hairline=True,
            )
            
            # Set additional anti-detection parameters
            self.driver.execute_cdp_cmd(
                "Network.setUserAgentOverride",
                {"userAgent": self._get_baidu_user_agent()}
            )
            
        except Exception as e:
            logger.error(f"Browser initialization failed: {str(e)}")
            raise

    def _get_baidu_user_agent(self) -> str:
        """Get Baidu-compatible user agents"""
        return random.choice([
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko"  # IE11
        ])

    def _check_for_captcha(self) -> bool:
        """Check if Baidu is showing CAPTCHA"""
        try:
            return bool(self.driver.find_elements(By.ID, "captcha"))
        except:
            return False

    async def _random_delay(self):
        """Add random delay between requests"""
        delay = random.uniform(*self._request_delay)
        await asyncio.sleep(delay)
    
    # ========== GOOGLE SCRAPER ========== #
class GoogleScrapeError(Exception):
    """Custom exception for Google scraping errors"""
    pass

class GoogleScraper:

    def __init__(
        self,
        progress: Any,
        api_key: str = None,
        lang_config: dict = None,
        proxy_manager: Any = None
    ):
        self.progress = progress
        self.api_key = api_key
        self.lang_config = lang_config or {
            "code": "en",
            "titles": {},
            "google_domain": "google.com",
            "country_code": "us"
        }
        self.proxy_manager = proxy_manager
        self.session = None
        self.driver = None
        self.retry_count = 0
        self.ua = UserAgent()
        self.cache = {}

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Async context manager exit with cleanup"""
        await self.cleanup()

    async def cleanup(self):
        """Clean up all resources"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
            if self.driver:
                self.driver.quit()
        except Exception as e:
            print(f"Cleanup error: {str(e)}")

    async def _random_delay(self):
        """Add random delay between requests with exponential backoff"""
        base_delay = random.uniform(0.5, 2.5)
        if self.retry_count > 0:
            backoff_factor = min(2 ** self.retry_count, 30)
            total_delay = base_delay * backoff_factor
        else:
            total_delay = base_delay
        await asyncio.sleep(total_delay)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError))
    )
    async def scrape(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Main scraping method with multiple fallback strategies"""
        cache_key = f"{query['job_title']}_{query['location']}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        results = []
        try:
            titles = [query['job_title']]  # Simplified for example
            strategies = [self._scrape_via_api, self._scrape_direct, self._scrape_via_selenium]

            for title in titles:
                await self._random_delay()
                search_query = f"{title} {query['industry']} {query['location']}"
                url = self._build_google_url(search_query)

                for strategy in strategies:
                    try:
                        if strategy == self._scrape_via_api and not self.api_key:
                            continue
                        page_results = await strategy(url)
                        if page_results:
                            results.extend(page_results)
                            break
                    except Exception as e:
                        print(f"Strategy failed: {str(e)}")
                        self.retry_count += 1
                        continue

            self.cache[cache_key] = self._deduplicate_results(results)
            return self.cache[cache_key]
        
        except Exception as e:
            print(f"Google scraping failed: {str(e)}")
            return []
        finally:
            self.retry_count = 0

    async def _scrape_via_api(self, url: str) -> List[Dict[str, Any]]:
        """Scrape using ScrapingBee API"""
        if not self.session:
            self.session = aiohttp.ClientSession()

        params = {
            "api_key": self.api_key,
            "url": url,
            "render_js": "true",
            "wait": "5000"
        }

        try:
            async with self.session.get(
                "https://app.scrapingbee.com/api/v1",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    raise GoogleScrapeError(f"API Error {resp.status}: {error}")
                html = await resp.text()
                return self._parse_results(BeautifulSoup(html, 'html.parser'))
        except aiohttp.ClientError as e:
            raise GoogleScrapeError(f"Network error: {str(e)}")

    async def _scrape_direct(self, url: str) -> List[Dict[str, Any]]:
        """Direct scraping with headers"""
        if not self.session:
            self.session = aiohttp.ClientSession()

        headers = self._get_headers()
        try:
            async with self.session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    raise GoogleScrapeError(f"HTTP Error {resp.status}")
                html = await resp.text()
                return self._parse_results(BeautifulSoup(html, 'html.parser'))
        except aiohttp.ClientError as e:
            raise GoogleScrapeError(f"Direct request failed: {str(e)}")

    async def _scrape_via_selenium(self, url: str) -> List[Dict[str, Any]]:
        """Selenium fallback method"""
        if not self.driver:
            await self._init_selenium()

        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".tF2Cxc, .g, .rc"))
            )
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            return self._parse_results(soup)
        except Exception as e:
            raise GoogleScrapeError(f"Selenium failed: {str(e)}")

    async def _init_selenium(self):
        """Initialize Selenium browser"""
        chrome_options = ChromeOptions()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(f"user-agent={self._get_user_agent()}")
        
        if True:  # Set your headless condition
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1920,1080")

        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            stealth(
                self.driver,
                languages=[self.lang_config['code']],
                vendor="Google Inc.",
                platform="Win32",
                fix_hairline=True,
            )
        except Exception as e:
            raise GoogleScrapeError(f"Selenium init failed: {str(e)}")

    def _get_headers(self) -> Dict[str, str]:
        """Generate request headers"""
        return {
            "User-Agent": self._get_user_agent(),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": f"{self.lang_config['code']};q=0.9",
        }

    def _get_user_agent(self) -> str:
        """Get random user agent"""
        return self.ua.random

    def _build_google_url(self, query: str) -> str:
        """Build Google search URL"""
        domain = self.lang_config.get("google_domain", "google.com")
        return (
            f"https://{domain}/search?"
            f"q={quote_plus(query)}&"
            f"hl={self.lang_config['code']}&"
            f"num=100"
        )

    def _parse_results(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse search results"""
        results = []
        for result in soup.select(".tF2Cxc, .g, .rc"):
            try:
                link = result.select_one("a")["href"]
                title = result.select_one("h3").text.strip()
                snippet = result.select_one(".IsZvec, .st, .s")
                results.append({
                    "title": title,
                    "url": self._clean_url(link),
                    "snippet": snippet.text.strip() if snippet else "",
                    "source": "Google",
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                continue
        return results

    def _clean_url(self, url: str) -> str:
        """Clean tracking parameters from URL"""
        if url.startswith("/url?q="):
            url = url[7:].split('&')[0]
        return url.split('&')[0].split('?')[0]

    def _deduplicate_results(self, results: List[Dict]) -> List[Dict]:
        """Remove duplicate results"""
        seen = set()
        unique = []
        for result in results:
            key = (result["url"], result["title"])
            if key not in seen:
                seen.add(key)
                unique.append(result)
        return unique
        
# ========== LEAD ENRICHMENT ========== #
class LeadEnricher:
    def __init__(self, progress: ProgressTracker, api_keys: dict):
        self.progress = progress
        self.api_keys = api_keys

    async def enrich(self, lead: Dict) -> Dict:
        """Enhance lead data with additional info"""
        if not Config.AI_ENRICHMENT:
            return lead

        task_name = f"Enriching {lead.get('name', 'lead')[:15]}..."
        self.progress.add_task(task_name, total=100)
        
        try:
            # Email guessing
            lead["email"] = await self._guess_email(lead)
            
            # Company data
            if "company" not in lead and "hunter.io" in self.api_keys:
                lead["company"] = await self._find_company(lead)
            
            self.progress.update(task_name, advance=100)
            return lead
        except Exception as e:
            console.print(f"[yellow]Enrichment failed: {str(e)}[/]")
            return lead  # Make sure to return the lead even if enrichment fails

    async def _guess_email(self, lead: Dict) -> str:
        """Generate probable email address"""
        if "name" not in lead or "company" not in lead:
            return ""

        try:
            name_parts = lead["name"].lower().split()
            if len(name_parts) < 2:
                return []
                
            first, last = name_parts[0], name_parts[-1]
            domain = self._extract_domain(lead["company"])
            if not domain:
                return []

            patterns = [
                f"{first}.{last}@{domain}",       # john.doe@company.com
                f"{first[0]}{last}@{domain}",     # jdoe@company.com
                f"{first}@{domain}",              # john@company.com
                f"{first}_{last}@{domain}",       # john_doe@company.com
                f"{first[0]}.{last}@{domain}",    # j.doe@company.com
            ]
            
            return [email for email in patterns if self._validate_email(email)]
        except Exception as e:
            logger.debug(f"Email guessing failed: {str(e)}")
            return []

    async def _find_company(self, lead: Dict) -> str:
        """Find company using Hunter.io"""
        if "hunter.io" not in self.api_keys:
            return ""
        
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.hunter.io/v2/domain-search?domain={lead['url'].split('/in/')[1].split('/')[0]}&api_key={self.api_keys['hunter.io']}"
                async with session.get(url) as resp:
                    data = await resp.json()
                    return data.get('data', {}).get('organization', '')
        except:
            return ""
            
       # ========== MAIN GENERATOR ========== #
class LeadGeneratorX:
    def __init__(self, api_keys: dict = None, lang_config: dict = None, 
                 progress: ProgressTracker = None, proxy_manager: ProxyManager = None):
        self.progress = progress or ProgressTracker()
        self.api_keys = api_keys or {}
        self.lang_config = lang_config or PolyglotScraper.LANGUAGE_MAP["English"]
        self._cache = {}  # For caching API responses
        self._last_calls = {}  # For rate limiting
        self.ua = UserAgent()  # For realistic headers
        self.proxy_manager = proxy_manager  # Store it
        self.enricher = LeadEnricher(self.progress, self.api_keys)
        self.scrapers = []

    async def scrape_earthwide(self, job_title: str, industry: str, location: str) -> List[Dict]:
        """Orchestrate global scraping"""
        regions = await GeoExplorer.select_region()
        all_leads = []
        
        with self.progress:
            if not regions:  # Global search
                all_leads = await self._scrape_region(job_title, industry, location)
            else:
                for country in regions:
                    task_name = f"Scraping {country}"
                    self.progress.add_task(task_name)
                    leads = await self._scrape_region(
                        job_title,
                        industry,
                        f"{location}, {country}" if "," in location else country
                    )
                    all_leads.extend(leads)
                    self.progress.update(task_name, advance=100)

        return await self._process_results(all_leads)

    async def _scrape_region(self, job_title: str, industry: str, location: str) -> List[Dict]:
        query = {
            "job_title": self._localize_title(job_title),
            "industry": industry,
            "location": location,
            "hl": self.lang_config["code"]
        }
        
        # Pass proxy_manager to scrapers that need it
        linkedin = LinkedInScraper(
            self.progress, 
            self.api_keys.get("ScrapingBee"), 
            self.lang_config,
            self.proxy_manager  # Pass it here
        )
        google = GoogleScraper(
            self.progress, 
            self.api_keys.get("ScrapingBee"), 
            self.lang_config
        )
        baidu = BaiduScraper(
            self.progress, 
            self.lang_config,
            self.proxy_manager  # And here if Baidu needs proxies
        )
        
        # Run concurrently (add Baidu to the gather)
        results = await asyncio.gather(
            linkedin.scrape(query),
            google.scrape(query),
            baidu.scrape(query)  # NEW
        )
        return [lead for sublist in results for lead in sublist]

    def _localize_title(self, title: str) -> str:
        """Translate job title based on language"""
        return PolyglotScraper.translate_title(title, self.lang_config)

    async def _process_results(self, results: List[Dict]) -> List[Dict]:
        """Deduplicate and enrich leads"""
        seen = set()
        enriched = []
        
        for lead in results:
            if lead["url"] not in seen:
                seen.add(lead["url"])
                enriched.append(await self.enricher.enrich(lead))
        
        return sorted(enriched, key=lambda x: x.get("score", 0), reverse=True)

    def cleanup(self):
        """Release all resources"""
        for scraper in self.scrapers:
            if hasattr(scraper, 'cleanup'):
                scraper.cleanup()
                
    async def _verify_email(self, email: str) -> bool:
        """Verify email existence using MailboxLayer API"""
        if "mailboxlayer" not in self.api_keys:
            return True  # Skip verification if no API key
            
        cache_key = f"email_verify_{email}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            async with aiohttp.ClientSession() as session:
                url = f"http://apilayer.net/api/check?email={email}&access_key={self.api_keys['mailboxlayer']}"
                async with session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        logger.warning(f"Email verification API error: HTTP {resp.status}")
                        return True  # Assume valid if API fails
                    
                    data = await resp.json()
                    is_valid = data.get('format_valid', False) and data.get('mx_found', False)
                    self._cache[cache_key] = is_valid  # Cache the result
                    return is_valid
                    
        except asyncio.TimeoutError:
            logger.warning("Email verification timed out")
            return True  # Assume valid on timeout
        except Exception as e:
            logger.error(f"Email verification failed: {str(e)}")
            return True  # Fail-safe: assume valid

    async def _get_company_details(self, domain: str) -> Dict:
        """Get full company data from Clearbit"""
        if "clearbit" not in self.api_keys:
            return {}
            
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://company.clearbit.com/v2/companies/find?domain={domain}"
                headers = {"Authorization": f"Bearer {self.api_keys['clearbit']}"}
                async with session.get(url, headers=headers, timeout=10) as resp:
                    return await resp.json()
        except Exception as e:
            logger.debug(f"Clearbit lookup failed: {str(e)}")
            return {}
            
    async def _find_social_media(self, name: str, company: str) -> Dict:
        """Find social profiles using FullContact API"""
        profiles = {}
        if "fullcontact" not in self.api_keys:
            return profiles
            
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://api.fullcontact.com/v3/person.enrich"
                headers = {"Authorization": f"Bearer {self.api_keys['fullcontact']}"}
                data = {"fullName": name, "company": company}
                
                async with session.post(url, json=data, headers=headers) as resp:
                    result = await resp.json()
                    if "socialProfiles" in result:
                        for profile in result["socialProfiles"]:
                            profiles[profile["type"].lower()] = profile["url"]
        except Exception:
            pass
            
        return profiles        
            
    async def _validate_phone(self, phone: str) -> bool:
        """Validate phone number using Twilio Lookup"""
        if "twilio" not in self.api_keys:
            return True
            
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://lookups.twilio.com/v1/PhoneNumbers/{phone}"
                auth = aiohttp.BasicAuth(self.api_keys["twilio_sid"], self.api_keys["twilio_token"])
                async with session.get(url, auth=auth) as resp:
                    return resp.status == 200
        except Exception:
            return False            
            
    def _normalize_data(self, lead: Dict) -> Dict:
        """Standardize all field formats"""
        normalized = lead.copy()
        
        # Name formatting
        if "name" in normalized:
            normalized["name"] = " ".join(
                part.capitalize() for part in normalized["name"].split()
            )
        
        # Phone formatting
        if "phones" in normalized:
            normalized["phones"] = [
                re.sub(r"[^\d+]", "", phone) 
                for phone in normalized["phones"]
            ]
        
        # Email lowercase
        if "email" in normalized:
            normalized["email"] = normalized["email"].lower()
            
        return normalized     
        
    async def _rate_limit(self, service: str):
        """Enforce API rate limits"""
        if service not in self._last_calls:
            self._last_calls[service] = datetime.now()
            return
            
        elapsed = (datetime.now() - self._last_calls[service]).total_seconds()
        min_delay = {
            "hunter": 0.5,
            "clearbit": 1.0,
            "fullcontact": 2.0
        }.get(service, 1.0)
        
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
        
        self._last_calls[service] = datetime.now()    
        
    async def enrich(self, lead: Dict) -> Dict:
        """Full enrichment workflow"""
        if not self._validate_lead(lead):
            return lead
            
        tasks = {
            "emails": self._find_emails(lead),
            "company": self._get_company_data(lead),
            "profiles": self._find_social_media(lead.get("name"), lead.get("company")),
            "phones": self._validate_phones(lead),
        }
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        enriched = lead.copy()
        for key, result in zip(tasks.keys(), results):
            if not isinstance(result, Exception):
                enriched[key] = result
                
        enriched["score"] = self._calculate_score(enriched)
        return self._normalize_data(enriched)    
        
    async def safe_api_call(self, url: str, service: str) -> Optional[Dict]:
        try:
            await self._rate_limit(service)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except Exception as e:
            logger.warning(f"API call failed ({service}): {str(e)}")
            return None
        
# ========== MAIN EXECUTION ========== #
async def main():
    """Enhanced main function with better user onboarding"""
    try:
        # Show welcome banner
        console.print(Panel.fit(
            "[bold blue]🌐 Sistine Chapel Lead Hunter v1.0 🌐[/]",
            subtitle="[green]Author: Don Giddy[/green]"
        ))
        
        # Explain what APIs do
        console.print(Panel.fit(
            "[bold]About API Keys:[/]\n"
            "This tool works best with API services that help:\n"
            "1. Bypass website blocks (ScrapingBee)\n"
            "2. Find email addresses (Hunter.io)\n"
            "3. Enrich company data (Clearbit)\n\n"
            "[yellow]You can skip all APIs but results will be limited[/]",
            border_style="blue"
        ))
        
        # Configure APIs
        api_keys = await APIVault.configure()
        
        # Explain what happens without APIs
        if not api_keys:
            console.print(Panel.fit(
                "[yellow]⚠ Warning[/]\n"
                "Running without APIs means:\n"
                "- Higher chance of being blocked\n"
                "- No email addresses will be found\n"
                "- Limited company information\n"
                "- Fewer overall results",
                border_style="yellow"
            ))
            confirm = await questionary.confirm(
                "Continue with limited functionality?",
                default=False
            ).unsafe_ask_async()
            if not confirm:
                return
                
        # Rest of your existing main() function...
        Config.validate()
        lang_config = await PolyglotScraper.select_language()
        proxy_manager = ProxyManager(Config.PROXY_LIST) if Config.PROXY_ENABLED else None
        
        # Get search parameters with better explanations
        params = await questionary.form(
            job_title=questionary.text("Job title to search for:"),
            industry=questionary.text("Industry (e.g., 'technology', 'healthcare'):"),
            location=questionary.text("Location (e.g., 'New York', 'Germany'):")
        ).unsafe_ask_async()
        
        # Run with progress tracking
        with ProgressTracker() as tracker:
            generator = LeadGeneratorX(
                api_keys=api_keys,
                lang_config=lang_config,
                progress=tracker,
                proxy_manager=proxy_manager
            )
            
            results = await generator.scrape_earthwide(**params)
            
            # Save results with user feedback
            if results:
                json_file = Config.OUTPUT_FILE
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                    
                console.print(Panel.fit(
                    f"[green]✓ Success! Found {len(results)} leads[/]\n"
                    f"Saved to: [bold]{json_file}[/]",
                    title="Complete"
                ))
                
                # Show sample result
                console.print(Panel.fit(
                    f"[bold]Sample Lead:[/]\n"
                    f"Name: {results[0].get('name', 'N/A')}\n"
                    f"Title: {results[0].get('title', 'N/A')}\n"
                    f"Company: {results[0].get('company', 'N/A')}\n"
                    f"Email: {results[0].get('email', '[red]No email (Hunter.io API needed)[/]')}",
                    border_style="green"
                ))
            else:
                console.print(Panel.fit(
                    "[red]No results found[/]\n"
                    "Possible reasons:\n"
                    "- Website blocks (try ScrapingBee API)\n"
                    "- Too specific search terms\n"
                    "- No profiles match your criteria",
                    border_style="red"
                ))
                
    except KeyboardInterrupt:
        console.print("\n[yellow]🚨 Graceful shutdown...[/]")
    except Exception as e:
        logger.exception("Unexpected error")
        console.print(f"[red]Error: {str(e)}[/]")
    finally:
        if 'generator' in locals():
            generator.cleanup()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main()) 
