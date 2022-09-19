from __future__ import annotations

from collections import Counter, defaultdict
import itertools
import logging
import random
from typing import Any, cast

import voluptuous as vol

from homeassistant.components import light
from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_MODE,
    ATTR_COLOR_TEMP,
    ATTR_EFFECT,
    ATTR_EFFECT_LIST,
    ATTR_FLASH,
    ATTR_HS_COLOR,
    ATTR_MAX_MIREDS,
    ATTR_MIN_MIREDS,
    ATTR_RGB_COLOR,
    ATTR_RGBW_COLOR,
    ATTR_RGBWW_COLOR,
    ATTR_SUPPORTED_COLOR_MODES,
    ATTR_TRANSITION,
    ATTR_WHITE,
    ATTR_XY_COLOR,
    PLATFORM_SCHEMA,
    ColorMode,
    LightEntity,
    LightEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_ENTITY_ID,
    ATTR_SUPPORTED_FEATURES,
    CONF_ENTITIES,
    CONF_NAME,
    CONF_UNIQUE_ID,
    SERVICE_TURN_OFF,
    SERVICE_TURN_ON,
    STATE_ON,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers import config_validation as cv, entity_registry as er
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from homeassistant.components.group import GroupEntity
from homeassistant.components.group.util import find_state_attributes, mean_tuple, reduce_attribute

from .const import *

# based on the HASS internal light group code

# No limit on parallel updates to enable a group calling another group
PARALLEL_UPDATES = 0

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Optional(CONF_UNIQUE_ID): cv.string,
        vol.Required(CONF_GROUPS): [{
            vol.Required(CONF_ENTITIES): cv.entities_domain(light.DOMAIN),
            vol.Required(CONF_MIN_INTENSITY): int,
            vol.Required(CONF_MAX_INTENSITY): int,
            vol.Optional(CONF_ORDER): int,
        }],
    }
)

SUPPORT_GROUP_LIGHT = (
    LightEntityFeature.EFFECT | LightEntityFeature.FLASH | LightEntityFeature.TRANSITION
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Initialize light.group platform."""
    async_add_entities(
        [
            ExtraDimmableLightGroup(
                config.get(CONF_UNIQUE_ID),
                config[CONF_NAME],
                config[CONF_GROUPS],
            )
        ]
    )


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Initialize Light Group config entry."""
    async_add_entities(
        [ExtraDimmableLightGroup(config_entry.entry_id, config_entry.title, config_entry.options[CONF_GROUPS])]
    )


FORWARDED_ATTRIBUTES = frozenset(
    {
        ATTR_COLOR_TEMP,
        ATTR_EFFECT,
        ATTR_FLASH,
        ATTR_HS_COLOR,
        ATTR_RGB_COLOR,
        ATTR_RGBW_COLOR,
        ATTR_RGBWW_COLOR,
        ATTR_TRANSITION,
        ATTR_WHITE,
        ATTR_XY_COLOR,
    }
)


class ExtraDimmableLightGroup(GroupEntity, LightEntity):
    """Representation of a light group."""

    _attr_available = False
    _attr_icon = "mdi:lightbulb-group"
    _attr_max_mireds = 500
    _attr_min_mireds = 154
    _attr_should_poll = False

    def __init__(
        self, unique_id: str | None, name: str, groups: list[dict]
    ) -> None:
        """Initialize a light group."""
        self._entity_ids = [eid for group in groups for eid in group[CONF_ENTITIES]]
        self._bounds = {eid: (group[CONF_MIN_INTENSITY], group[CONF_MAX_INTENSITY]) for group in groups for eid in group[CONF_ENTITIES]}
        self._states = {eid: None for eid in self._entity_ids}
        ranked_groups = sorted(groups, key=lambda group: (CONF_ORDER in group, group.get(CONF_ORDER)))
        ranked_groups.reverse()
        self._ranking = [group[CONF_ENTITIES] for group in ranked_groups]
        group_min_intensities = [len(group[CONF_ENTITIES]) * group[CONF_MIN_INTENSITY] for group in ranked_groups]
        min_total_intensity = sum(group_min_intensities)
        self._max_intensity = sum(len(group[CONF_ENTITIES]) * group[CONF_MAX_INTENSITY] for group in ranked_groups)
        self._brightness_key = []
        for bri in range(0, 256):
            target = max(bri - 3, 0) / 252 * self._max_intensity
            if target >= min_total_intensity:
                self._brightness_key += [{"forward": True}]
                continue
            i = 0
            cum = 0
            rem = 0
            while i < len(groups):
                oldcum = cum
                cum += group_min_intensities[i]
                if cum >= target:
                    rem = target - oldcum
                    break
                i += 1
            if i == len(groups):
                i -= 1
                rem = group_min_intensities[i]
            totbulbs = len(ranked_groups[i][CONF_ENTITIES])
            minint = ranked_groups[i][CONF_MIN_INTENSITY]
            maxint = ranked_groups[i][CONF_MAX_INTENSITY]
            nbulbs = int(rem / minint)
            if nbulbs > totbulbs:
                nbulbs = totbulbs
            bulb_int = 0
            if nbulbs != 0:
                bulb_int = rem / nbulbs
            bulb_bri = max(round((bulb_int - minint) / maxint * 252), 0) + 3
            if i == 0 and nbulbs == 0:
                nbulbs = 1
            self._brightness_key += [{
                "forward": False,
                "group": i,
                "bulbs": nbulbs,
                "brightness": bulb_bri,
            }]

        self._attr_name = name
        self._attr_extra_state_attributes = {ATTR_ENTITY_ID: self._entity_ids}
        self._attr_unique_id = unique_id

    async def async_added_to_hass(self) -> None:
        """Register callbacks."""

        @callback
        def async_state_changed_listener(event: Event) -> None:
            """Handle child updates."""
            self.async_set_context(event.context)
            self.async_defer_or_update_ha_state()

        self.async_on_remove(
            async_track_state_change_event(
                self.hass, self._entity_ids, async_state_changed_listener
            )
        )

        await super().async_added_to_hass()

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Forward the turn_on command to all lights in the light group."""
        data = {
            key: value for key, value in kwargs.items() if key in FORWARDED_ATTRIBUTES
        }

        forward = ATTR_BRIGHTNESS not in kwargs
        if not forward:
            forward = self._brightness_key[kwargs[ATTR_BRIGHTNESS]]["forward"]

        if forward:
            data[ATTR_ENTITY_ID] = self._entity_ids
            if ATTR_BRIGHTNESS in kwargs:
                data[ATTR_BRIGHTNESS] = kwargs[ATTR_BRIGHTNESS]

            _LOGGER.debug("Forwarded turn_on command: %s", data)

            await self.hass.services.async_call(
                light.DOMAIN,
                SERVICE_TURN_ON,
                data,
                blocking=True,
                context=self._context,
            )
        else:
            key = self._brightness_key[kwargs[ATTR_BRIGHTNESS]]
            group = key["group"]
            nbulbs = key["bulbs"]
            bri = key["brightness"]
            eids = self._ranking[group]
            totbulbs = len(eids)
            on_eids = [eid for eid in eids if self._states[eid]]
            off_eids = [eid for eid in eids if not self._states[eid]]
            if len(on_eids) < nbulbs:
                diff = nbulbs - len(on_eids)
                moved = random.choices(off_eids, k=diff)
                on_eids += moved
                off_eids = list(set(off_eids) - set(moved))
            elif len(on_eids) > nbulbs:
                diff = len(on_eids) - nbulbs
                moved = random.choices(on_eids, k=diff)
                off_eids += moved
                on_eids = list(set(on_eids) - set(moved))
            min_eids = [eid for group in self._ranking[:group] for eid in group]
            off_eids += [eid for group in self._ranking[group+1:] for eid in group]

            on_data = data | {
                ATTR_ENTITY_ID: on_eids,
                ATTR_BRIGHTNESS: bri,
            }
            min_data = data | {
                ATTR_ENTITY_ID: on_eids,
                ATTR_BRIGHTNESS: 3,
            }
            off_data = {ATTR_ENTITY_ID: off_eids}
            if ATTR_TRANSITION in kwargs:
                off_data[ATTR_TRANSITION] = kwargs[ATTR_TRANSITION]

            _LOGGER.debug("Processed turn_on command: %s", data)

            await self.hass.services.async_call(
                light.DOMAIN,
                SERVICE_TURN_OFF,
                off_data,
                blocking=True,
                context=self._context,
            )
            await self.hass.services.async_call(
                light.DOMAIN,
                SERVICE_TURN_ON,
                min_data,
                blocking=True,
                context=self._context,
            )
            await self.hass.services.async_call(
                light.DOMAIN,
                SERVICE_TURN_ON,
                on_data,
                blocking=True,
                context=self._context,
            )

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Forward the turn_off command to all lights in the light group."""
        data = {ATTR_ENTITY_ID: self._entity_ids}

        if ATTR_TRANSITION in kwargs:
            data[ATTR_TRANSITION] = kwargs[ATTR_TRANSITION]

        await self.hass.services.async_call(
            light.DOMAIN,
            SERVICE_TURN_OFF,
            data,
            blocking=True,
            context=self._context,
        )

    @callback
    def async_update_group_state(self) -> None:
        """Query all members and determine the light group state."""
        states_dict = {eid: state
            for eid in self._entity_ids if (state := self.hass.states.get(eid)) is not None}
        self._states = {eid: state.state == STATE_ON for eid, state in states_dict.items()}

        states = list(states_dict.values())
        on_states = [state for state in states if state.state == STATE_ON]

        valid_state = any(
            state.state not in (STATE_UNKNOWN, STATE_UNAVAILABLE) for state in states
        )

        if not valid_state:
            # Set as unknown if any / all member is unknown or unavailable
            self._attr_is_on = None
        else:
            # Set as ON if any / all member is ON
            self._attr_is_on = any(state.state == STATE_ON for state in states)

        self._attr_available = any(state.state != STATE_UNAVAILABLE for state in states)

        current_intensity = 0
        for eid, state in states_dict.items():
            if state.state == STATE_ON and (bri := state.attributes.get(ATTR_BRIGHTNESS)) is not None:
                minint, maxint = self._bounds[eid]
                current_intensity += (maxint - minint) * max(bri - 3, 0) / 252 + minint
        self._attr_brightness = round(current_intensity / self._max_intensity * 255)

        self._attr_hs_color = reduce_attribute(
            on_states, ATTR_HS_COLOR, reduce=mean_tuple
        )
        self._attr_rgb_color = reduce_attribute(
            on_states, ATTR_RGB_COLOR, reduce=mean_tuple
        )
        self._attr_rgbw_color = reduce_attribute(
            on_states, ATTR_RGBW_COLOR, reduce=mean_tuple
        )
        self._attr_rgbww_color = reduce_attribute(
            on_states, ATTR_RGBWW_COLOR, reduce=mean_tuple
        )
        self._attr_xy_color = reduce_attribute(
            on_states, ATTR_XY_COLOR, reduce=mean_tuple
        )

        self._attr_color_temp = reduce_attribute(on_states, ATTR_COLOR_TEMP)
        self._attr_min_mireds = reduce_attribute(
            states, ATTR_MIN_MIREDS, default=154, reduce=min
        )
        self._attr_max_mireds = reduce_attribute(
            states, ATTR_MAX_MIREDS, default=500, reduce=max
        )

        self._attr_effect_list = None
        all_effect_lists = list(find_state_attributes(states, ATTR_EFFECT_LIST))
        if all_effect_lists:
            # Merge all effects from all effect_lists with a union merge.
            self._attr_effect_list = list(set().union(*all_effect_lists))
            self._attr_effect_list.sort()
            if "None" in self._attr_effect_list:
                self._attr_effect_list.remove("None")
                self._attr_effect_list.insert(0, "None")

        self._attr_effect = None
        all_effects = list(find_state_attributes(on_states, ATTR_EFFECT))
        if all_effects:
            # Report the most common effect.
            effects_count = Counter(itertools.chain(all_effects))
            self._attr_effect = effects_count.most_common(1)[0][0]

        self._attr_color_mode = None
        all_color_modes = list(find_state_attributes(on_states, ATTR_COLOR_MODE))
        if all_color_modes:
            # Report the most common color mode, select brightness and onoff last
            color_mode_count = Counter(itertools.chain(all_color_modes))
            if ColorMode.ONOFF in color_mode_count:
                color_mode_count[ColorMode.ONOFF] = -1
            if ColorMode.BRIGHTNESS in color_mode_count:
                color_mode_count[ColorMode.BRIGHTNESS] = 0
            self._attr_color_mode = color_mode_count.most_common(1)[0][0]

        self._attr_supported_color_modes = None
        all_supported_color_modes = list(
            find_state_attributes(states, ATTR_SUPPORTED_COLOR_MODES)
        )
        if all_supported_color_modes:
            # Merge all color modes.
            self._attr_supported_color_modes = cast(
                set[str], set().union(*all_supported_color_modes)
            )

        self._attr_supported_features = 0
        for support in find_state_attributes(states, ATTR_SUPPORTED_FEATURES):
            # Merge supported features by emulating support for every feature
            # we find.
            self._attr_supported_features |= support
        # Bitwise-and the supported features with the GroupedLight's features
        # so that we don't break in the future when a new feature is added.
        self._attr_supported_features &= SUPPORT_GROUP_LIGHT
