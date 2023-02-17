package YSB

import java.io.Serializable


class CampaignAd extends Serializable {
  var ad_id: String = null
  var campaign_id: String = null

  // constructor
  def this {
    this()
    ad_id = null
    campaign_id = null
  }

  // constructor
  def this(_ad_id: String, _compagin_id: String) {
    this()
    ad_id = _ad_id
    campaign_id = _compagin_id
  }
}
