from dataclasses import dataclass

from pyscript import document, window, workers  # type: ignore


@dataclass(frozen=True, slots=True)
class Modifiers:
    liked_songs_modifier: float
    ug_url_modifier: float
    wywrota_url_modifier: float


class ButtonManager:
    def __init__(self, element_id: str, *, disabled: bool) -> None:
        self.element_id = element_id
        self.html_element = document.getElementById(self.element_id)
        self.disabled = disabled
        if self.disabled:
            self.html_element.setAttribute("disabled", "")

    def disable(self) -> None:
        if not self.disabled:
            self.html_element.setAttribute("disabled", "")
            self.disabled = True

    def enable(self) -> None:
        if self.disabled:
            self.html_element.removeAttribute("disabled")
            self.disabled = False

    def __enter__(self) -> None:
        self.disable()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.enable()


class Settings:
    def __init__(
        self,
        liked_songs_modifier_element_id: str,
        ug_url_modifier_element_id: str,
        wywrota_url_modifier_element_id: str,
    ) -> None:
        self.liked_songs_modifier_element = document.getElementById(liked_songs_modifier_element_id)
        self.ug_url_modifier_element = document.getElementById(ug_url_modifier_element_id)
        self.wywrota_url_modifier_element = document.getElementById(wywrota_url_modifier_element_id)

    def get_modifiers(self) -> Modifiers:
        return Modifiers(
            liked_songs_modifier=float(self.liked_songs_modifier_element.value),
            ug_url_modifier=float(self.ug_url_modifier_element.value),
            wywrota_url_modifier=float(self.wywrota_url_modifier_element.value),
        )


# buttons
shuffle_button_manager = ButtonManager(element_id="button-shuffle", disabled=True)
back_button_manager = ButtonManager(element_id="button-back", disabled=True)


# modals
loading_database_modal = document.getElementById("loading-database")


# table objects
table_shuffle_results = document.getElementById("div-results")
table_search_results = document.getElementById("div-results-search")


# settings element
settings = Settings(
    liked_songs_modifier_element_id="liked-modifier",
    ug_url_modifier_element_id="ug-modifier",
    wywrota_url_modifier_element_id="wywrota-modifier",
)


# Define functions called from HTML.
async def ui_new_shuffle(*args, **kwargs) -> None:
    modifiers = settings.get_modifiers()
    with shuffle_button_manager, back_button_manager:
        table = await backend_worker.new_shuffle(
            liked_songs_modifier=modifiers.liked_songs_modifier,
            ug_url_modifier=modifiers.ug_url_modifier,
            wywrota_url_modifier=modifiers.wywrota_url_modifier,
        )
        table_shuffle_results.innerHTML = table


async def ui_load_previous_songs(*args, **kwargs) -> None:
    with shuffle_button_manager, back_button_manager:
        how_many_previous_available, table = await backend_worker.load_previous_songs()
        table_shuffle_results.innerHTML = table
    if how_many_previous_available == 0:
        back_button_manager.disable()


async def ui_new_search(*args, **kwargs) -> None:
    search_term = str(document.getElementById("search-input").value).replace(" - ", " ").strip()
    result = await backend_worker.new_search(search_term)
    table_search_results.innerHTML = result


async def ui_new_search_on_keypress(event) -> None:
    if event.key == "Enter":
        await ui_new_search()
    elif event.key == "Escape":
        document.getElementById("search-input").value = ""


# initialize
window.console.log("Initializing backend worker...")
backend_worker = await workers["backend-worker"]
window.console.log("Creating database...")
await backend_worker.init_data_store_and_table()
window.console.log("Fetching statistics...")
number_of_songs, ug_tabs, wywrota_tabs = await backend_worker.get_stats()
document.getElementById("span-count").textContent = f"{number_of_songs:,}"
document.getElementById("span-count-wywrota").textContent = f"{wywrota_tabs:,}"
document.getElementById("span-count-ug").textContent = f"{ug_tabs:,}"
window.console.log("Prepare UI")
shuffle_button_manager.enable()
loading_database_modal.close()
window.console.log("Initialization complete.")
