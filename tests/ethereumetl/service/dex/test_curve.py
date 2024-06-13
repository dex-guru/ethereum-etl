from ethereumetl.service.dex.curve.curve import CurveAmm


def test_curve_init(web3):
    curve = CurveAmm(web3, 1)
    assert curve is not None
