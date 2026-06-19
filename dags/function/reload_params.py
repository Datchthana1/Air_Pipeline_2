from airflow.models.param import Param


def reload_params(default_mode: str = "latest") -> dict:
    return {
        "reload_mode": Param(
            default=default_mode,
            type="string",
            enum=["latest", "day", "range", "full"],
            description="latest=snapshot ล่าสุด | day=วันเดียว | range=ช่วงวันที่ | full=ทั้งหมด",
        ),
        "reload_date": Param(default="", type=["null", "string"], description="โหมด day: YYYY-MM-DD"),
        "date_from":   Param(default="", type=["null", "string"], description="โหมด range: YYYY-MM-DD (เริ่ม)"),
        "date_to":     Param(default="", type=["null", "string"], description="โหมด range: YYYY-MM-DD (สิ้นสุด)"),
        "station_id":  Param(default="", type=["null", "string"], description="เจาะจง 1 สถานี (เว้นว่าง=ทุกสถานี)"),
    }


RELOAD_CONF = {
    "reload_mode": "{{ dag_run.conf.get('reload_mode') or params.reload_mode }}",
    "reload_date": "{{ dag_run.conf.get('reload_date') or params.reload_date }}",
    "date_from":   "{{ dag_run.conf.get('date_from')   or params.date_from }}",
    "date_to":     "{{ dag_run.conf.get('date_to')     or params.date_to }}",
    "station_id":  "{{ dag_run.conf.get('station_id')  or params.station_id }}",
}
